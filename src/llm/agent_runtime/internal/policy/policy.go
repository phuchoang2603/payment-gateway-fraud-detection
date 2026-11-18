package policy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/jsonschema-go/jsonschema"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/agent-runtime/internal/caller"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/plugin"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/genai"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Limits struct {
	ToolCalls   int `json:"toolCalls"`
	RemoteCalls int `json:"remoteCalls"`
}

type Contract struct {
	Schema    json.RawMessage `json:"schema"`
	Forbidden []string        `json:"forbidden"`
	resolved  *jsonschema.Resolved
}

type Profile struct {
	Tools        []string                     `json:"tools"`
	Transfers    []string                     `json:"transfers"`
	RemoteSkills []string                     `json:"remoteSkills"`
	Bind         map[string]map[string]string `json:"bind"`
	FromUser     map[string][]string          `json:"fromUser"`
	Limits       Limits                       `json:"limits"`
	Input        *Contract                    `json:"input"`
	Output       *Contract                    `json:"output"`
}

type RemoteSkill struct {
	URL         string `json:"url"`
	Description string `json:"description"`
}

type Config struct {
	Issuers      caller.Issuers         `json:"issuers"`
	RemoteOrigin string                 `json:"remoteOrigin"`
	Profiles     map[string]Profile     `json:"profiles"`
	RemoteSkills map[string]RemoteSkill `json:"remoteSkills"`
}

type invocation struct {
	sync.Mutex
	profile  Profile
	name     string
	identity caller.Identity
	tools    int
	remote   int
	user     []string
	results  []any
}

func (r *invocation) record(author string, content *genai.Content) {
	if content == nil {
		return
	}
	for _, part := range content.Parts {
		if part == nil {
			continue
		}
		if author == "user" && part.Text != "" {
			r.user = append(r.user, part.Text)
		}
		if part.FunctionResponse != nil {
			r.results = append(r.results, part.FunctionResponse.Response)
		}
	}
}

type Policy struct {
	config      Config
	root        string
	invocations sync.Map
	denials     *prometheus.CounterVec
}

func Load(raw, root string) (*Policy, error) {
	var config Config
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode agent policy: %w", err)
	}
	if len(config.Profiles) == 0 || config.Issuers.Gateway == "" {
		return nil, fmt.Errorf("agent policy requires profiles and gateway issuer")
	}
	if len(config.RemoteSkills) > 0 && config.RemoteOrigin == "" {
		return nil, fmt.Errorf("remote skills require the internal gateway origin")
	}
	for name, profile := range config.Profiles {
		if profile.Limits.ToolCalls <= 0 || profile.Limits.RemoteCalls < 0 {
			return nil, fmt.Errorf("invalid limits for profile %s", name)
		}
		for _, skill := range profile.RemoteSkills {
			if _, ok := config.RemoteSkills[skill]; !ok {
				return nil, fmt.Errorf("unknown remote skill %s", skill)
			}
		}
		for _, contract := range []*Contract{profile.Input, profile.Output} {
			if contract == nil {
				continue
			}
			var schema jsonschema.Schema
			if err := json.Unmarshal(contract.Schema, &schema); err != nil {
				return nil, fmt.Errorf("decode contract schema for profile %s: %w", name, err)
			}
			resolved, err := schema.Resolve(nil)
			if err != nil {
				return nil, fmt.Errorf("resolve contract schema for profile %s: %w", name, err)
			}
			contract.resolved = resolved
		}
	}
	denials := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "agent_policy_denials_total", Help: "Runtime policy refusals."}, []string{"agent", "profile", "rule"})
	return &Policy{config: config, root: root, denials: denials}, nil
}

func (p *Policy) ConfigureRemoteSkills(config *configv1.ADKConfig) error {
	config.Agent.RemoteAgents = nil
	for name, skill := range p.config.RemoteSkills {
		allowed := false
		for _, profile := range p.config.Profiles {
			allowed = allowed || slices.Contains(profile.RemoteSkills, name)
		}
		if !allowed {
			continue
		}
		endpoint, err := url.Parse(skill.URL)
		if err != nil || p.config.RemoteOrigin == "" || endpoint.Scheme+"://"+endpoint.Host != p.config.RemoteOrigin || endpoint.Path == "" {
			return fmt.Errorf("remote skill must target the internal gateway route")
		}
		config.Agent.RemoteAgents = append(config.Agent.RemoteAgents, &configv1.ADKRemoteAgentConfig{
			Name: name, Url: skill.URL, Description: skill.Description, IsolateSessions: true, Timeout: durationpb.New(3 * time.Minute),
		})
	}
	if config.Runtime == nil {
		config.Runtime = &configv1.ADKRuntimeConfig{}
	}
	config.Runtime.TokenPropagation = &configv1.TokenPropagationConfig{Enabled: true}
	return nil
}

func (p *Policy) Plugin() (*plugin.Plugin, error) {
	return plugin.New(plugin.Config{
		Name: "agent_policy", BeforeRunCallback: p.BeforeRun, BeforeAgentCallback: p.BeforeAgent, BeforeModelCallback: p.BeforeModel, BeforeToolCallback: p.BeforeTool,
		OnEventCallback: p.OnEvent, AfterRunCallback: func(ctx agent.InvocationContext) { p.invocations.Delete(ctx.InvocationID()) },
	})
}

func (p Profile) offers(tool string) bool {
	if tool == "transfer_to_agent" {
		return len(p.Transfers) > 0
	}
	return slices.Contains(p.Tools, tool) || slices.Contains(p.RemoteSkills, tool)
}

func (p *Policy) BeforeModel(ctx agent.Context, req *model.LLMRequest) (*model.LLMResponse, error) {
	value, ok := p.invocations.Load(ctx.InvocationID())
	if !ok {
		return nil, p.deny(ctx, "unknown", "unknown_caller", "")
	}
	if req.Config == nil {
		return nil, nil
	}
	profile := value.(*invocation).profile
	var offered []*genai.Tool
	for _, entry := range req.Config.Tools {
		if entry == nil {
			continue
		}
		var declarations []*genai.FunctionDeclaration
		for _, declaration := range entry.FunctionDeclarations {
			if profile.offers(declaration.Name) {
				declarations = append(declarations, declaration)
			}
		}
		if len(declarations) == 0 {
			continue
		}
		kept := *entry
		kept.FunctionDeclarations = declarations
		offered = append(offered, &kept)
	}
	req.Config.Tools = offered
	return nil, nil
}

func (p *Policy) deny(ctx context.Context, profile, rule, tool string) error {
	attrs := []attribute.KeyValue{attribute.String("agent", p.root), attribute.String("profile", profile), attribute.String("rule", rule), attribute.String("tool", tool)}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Bool("policy.denied", true))
	span.AddEvent("policy.denied", trace.WithAttributes(attrs...))
	p.denials.WithLabelValues(p.root, profile, rule).Inc()
	return fmt.Errorf("policy denied: %s", rule)
}

func (p *Policy) selectProfile(ctx context.Context) (*invocation, error) {
	identity, err := caller.FromContext(ctx, p.config.Issuers)
	if err != nil {
		return nil, p.deny(ctx, "unknown", "unknown_caller", "")
	}
	profile, ok := p.config.Profiles[identity.Client]
	if !ok {
		return nil, p.deny(ctx, identity.Client, "unknown_caller", "")
	}
	return &invocation{profile: profile, name: identity.Client, identity: identity}, nil
}

func (p *Policy) BeforeRun(ctx agent.InvocationContext) (*genai.Content, error) {
	invocation, err := p.selectProfile(ctx)
	if err != nil {
		return nil, err
	}
	invocation.record("user", ctx.UserContent())
	if history := ctx.Session(); history != nil && history.Events() != nil {
		for event := range history.Events().All() {
			invocation.record(event.Author, event.Content)
		}
	}
	p.invocations.Store(ctx.InvocationID(), invocation)
	return nil, nil
}

func (p *Policy) BeforeAgent(ctx agent.Context) (*genai.Content, error) {
	if _, ok := p.invocations.Load(ctx.InvocationID()); !ok {
		return nil, p.deny(ctx, "unknown", "unknown_caller", "")
	}
	return nil, nil
}

func (p *Policy) BeforeTool(ctx agent.Context, tool tool.Tool, args map[string]any) (map[string]any, error) {
	value, ok := p.invocations.Load(ctx.InvocationID())
	if !ok {
		return map[string]any{"error": p.deny(ctx, "unknown", "unknown_caller", tool.Name()).Error()}, nil
	}
	run := value.(*invocation)
	run.Lock()
	defer run.Unlock()
	if err := p.checkTool(ctx, run, tool.Name(), args); err != nil {
		return map[string]any{"error": err.Error()}, nil
	}
	return nil, nil
}

func (p *Policy) checkTool(ctx agent.Context, run *invocation, name string, args map[string]any) error {
	deny := func(rule string) error { return p.deny(ctx, run.name, rule, name) }
	remote := slices.Contains(run.profile.RemoteSkills, name)
	if name == "transfer_to_agent" {
		target, _ := args["agent_name"].(string)
		if !slices.Contains(run.profile.Transfers, target) {
			return deny("transfer_not_allowed")
		}
	} else if !slices.Contains(run.profile.Tools, name) && !remote {
		return deny("tool_not_allowed")
	}
	for argument, source := range run.profile.Bind[name] {
		if source != "integration" || run.identity.Integration == "" {
			return deny("missing_bound_context")
		}
		if !bindArgument(args, argument, run.identity.Integration) {
			return deny("missing_bound_argument")
		}
	}
	for _, argument := range run.profile.FromUser[name] {
		value := argumentValue(args, argument)
		if value == "" || !run.fromConversation(value) {
			return deny("identifier_not_from_user")
		}
	}
	if run.tools >= run.profile.Limits.ToolCalls || (remote && run.remote >= run.profile.Limits.RemoteCalls) {
		return deny("call_limit_exceeded")
	}
	run.tools++
	if remote {
		run.remote++
	}
	return nil
}

func bindArgument(args map[string]any, path, value string) bool {
	key, nested, ok := strings.Cut(path, ".")
	if !ok {
		args[key] = value
		return true
	}
	object, ok := args[key].(map[string]any)
	if !ok {
		text, _ := args[key].(string)
		if json.Unmarshal([]byte(text), &object) != nil || object == nil {
			return false
		}
		args[key] = object
	}
	return bindArgument(object, nested, value)
}

func argumentValue(args map[string]any, path string) string {
	key, nested, ok := strings.Cut(path, ".")
	value := args[key]
	if !ok {
		text, _ := value.(string)
		return text
	}
	if text, ok := value.(string); ok {
		if json.Unmarshal([]byte(text), &value) != nil {
			return ""
		}
	}
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	return argumentValue(object, nested)
}

func (r *invocation) fromConversation(value string) bool {
	for _, text := range r.user {
		if containsIdentifier(text, value) {
			return true
		}
	}
	for _, result := range r.results {
		if containsValue(result, value) {
			return true
		}
	}
	return false
}

func containsIdentifier(text, value string) bool {
	identifierRune := func(r rune) bool {
		return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
	}
	for offset := 0; offset < len(text); {
		index := strings.Index(text[offset:], value)
		if index < 0 {
			return false
		}
		start := offset + index
		end := start + len(value)
		before, _ := utf8.DecodeLastRuneInString(text[:start])
		after, _ := utf8.DecodeRuneInString(text[end:])
		if (start == 0 || !identifierRune(before)) && (end == len(text) || !identifierRune(after)) {
			return true
		}
		offset = start + 1
	}
	return false
}

func containsValue(object any, value string) bool {
	switch typed := object.(type) {
	case string:
		return typed == value
	case map[string]any:
		for _, item := range typed {
			if containsValue(item, value) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsValue(item, value) {
				return true
			}
		}
	}
	return false
}

func (p *Policy) OnEvent(ctx agent.InvocationContext, event *session.Event) (*session.Event, error) {
	if value, ok := p.invocations.Load(ctx.InvocationID()); ok && event != nil {
		run := value.(*invocation)
		run.Lock()
		run.record(event.Author, event.Content)
		run.Unlock()
	}
	if boundary, ok := ctx.Value(boundaryKey{}).(*reviewBoundary); ok && event != nil && event.Author == p.root && event.IsFinalResponse() && !event.Partial && event.Content != nil {
		var text strings.Builder
		for _, part := range event.Content.Parts {
			if part != nil {
				text.WriteString(part.Text)
			}
		}
		boundary.final = text.String()
	}
	return nil, nil
}

var (
	errContractJSON      = errors.New("invalid contract JSON")
	errContractSchema    = errors.New("contract schema violation")
	errContractForbidden = errors.New("forbidden contract field")
)

func (o *Contract) validate(text string) error {
	var value any
	if json.Unmarshal([]byte(text), &value) != nil {
		return errContractJSON
	}
	if err := o.resolved.Validate(value); err != nil {
		return errContractSchema
	}
	var forbidden func(any) bool
	forbidden = func(value any) bool {
		switch typed := value.(type) {
		case map[string]any:
			for key, value := range typed {
				if slices.Contains(o.Forbidden, key) || forbidden(value) {
					return true
				}
			}
		case []any:
			for _, value := range typed {
				if forbidden(value) {
					return true
				}
			}
		}
		return false
	}
	if forbidden(value) {
		return errContractForbidden
	}
	return nil
}

func (p *Policy) HealthHandler() http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(p.denials)
	metrics := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			metrics.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
