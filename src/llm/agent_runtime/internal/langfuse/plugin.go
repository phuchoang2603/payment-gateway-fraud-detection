package langfuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/plugin"
	"google.golang.org/genai"
)

const (
	promptNameAttribute    = attribute.Key("langfuse.observation.prompt.name")
	promptVersionAttribute = attribute.Key("langfuse.observation.prompt.version")
	traceTagsAttribute     = attribute.Key("langfuse.trace.tags")
	invocationIDAttribute  = attribute.Key("gcp.vertex.agent.invocation_id")
	generateContentSpan    = "generate_content"
)

type PromptSource interface {
	Prompt(ctx context.Context, name, label string) (Prompt, error)
}

type choice struct {
	prompt Prompt
	// tag names the agent's A/B side on the trace, or is empty when its prompt runs no test.
	tag string
}

// Prompts sets each model call's system prompt from the calling agent's
// Langfuse prompt, so sub-agents compiled into the runtime use their own. It is
// both the ADK plugin and the span processor that links the call to the prompt
// version: ADK opens the generate_content span after the plugin returns, so the
// choice is kept per invocation until that span starts.
type Prompts struct {
	source PromptSource
	// ConversationFile holds the conversation's key for the A/B split. kagent
	// gives every conversation the same ADK session id inside its Substrate
	// actor, so the key is the actor's name, which Substrate projects into the
	// file on every run and restore. Without the file, the session id is used.
	ConversationFile string

	choices sync.Map
}

var _ sdktrace.SpanProcessor = (*Prompts)(nil)

func NewPrompts(source PromptSource) *Prompts {
	return &Prompts{source: source}
}

// PromptName is the Langfuse prompt of an ADK agent. kagent names the root
// agent after the Agent and each sub-agent after its binding, both with
// underscores for hyphens.
func PromptName(agentName string) string {
	return strings.ReplaceAll(agentName, "_", "-") + "-system-prompt"
}

func (p *Prompts) conversation(ctx agent.Context) string {
	if p.ConversationFile != "" {
		if name, err := os.ReadFile(p.ConversationFile); err == nil && len(bytes.TrimSpace(name)) > 0 {
			return string(bytes.TrimSpace(name))
		}
	}
	return ctx.SessionID()
}

func (p *Prompts) Plugin() (*plugin.Plugin, error) {
	return plugin.New(plugin.Config{
		Name:                "langfuse_prompts",
		BeforeModelCallback: p.BeforeModel,
		AfterRunCallback:    func(ctx agent.InvocationContext) { p.choices.Delete(ctx.InvocationID()) },
	})
}

func (p *Prompts) choose(ctx context.Context, name, sessionID string) (choice, error) {
	production, err := p.source.Prompt(ctx, name, Production)
	if err != nil {
		return choice{}, err
	}
	label := VariantLabel(production, sessionID)
	tag := ""
	if test := production.Config.ABTest; test != nil && test.Label != "" {
		tag = strings.TrimSuffix(name, "-system-prompt") + ":" + label
	}
	if label == Production {
		return choice{production, tag}, nil
	}
	variant, err := p.source.Prompt(ctx, name, label)
	if err != nil {
		return choice{}, err
	}
	return choice{variant, tag}, nil
}

func (p *Prompts) BeforeModel(ctx agent.Context, req *model.LLMRequest) (*model.LLMResponse, error) {
	name := PromptName(ctx.AgentName())
	chosen, err := p.choose(ctx, name, p.conversation(ctx))
	if err != nil {
		return nil, fmt.Errorf("langfuse prompt %s: %w", name, err)
	}
	if req.Config == nil {
		req.Config = &genai.GenerateContentConfig{}
	}
	if req.Config.SystemInstruction == nil {
		req.Config.SystemInstruction = &genai.Content{Role: genai.RoleUser}
	}
	instruction := req.Config.SystemInstruction
	instruction.Parts = append([]*genai.Part{{Text: chosen.prompt.Prompt}}, instruction.Parts...)
	p.choices.Store(ctx.InvocationID(), chosen)
	return nil, nil
}

func (p *Prompts) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	if !strings.HasPrefix(span.Name(), generateContentSpan) {
		return
	}
	for _, kv := range span.Attributes() {
		if kv.Key != invocationIDAttribute {
			continue
		}
		value, ok := p.choices.Load(kv.Value.AsString())
		if !ok {
			return
		}
		chosen := value.(choice)
		span.SetAttributes(
			promptNameAttribute.String(chosen.prompt.Name),
			promptVersionAttribute.Int(chosen.prompt.Version),
		)
		if chosen.tag != "" {
			span.SetAttributes(traceTagsAttribute.StringSlice([]string{chosen.tag}))
		}
		return
	}
}

func (p *Prompts) OnEnd(sdktrace.ReadOnlySpan) {}

func (p *Prompts) Shutdown(context.Context) error { return nil }

func (p *Prompts) ForceFlush(context.Context) error { return nil }
