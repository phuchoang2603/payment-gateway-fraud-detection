package langfuse

import (
	"context"
	"fmt"
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
	label  string
}

// Prompts sets each model call's system prompt from Langfuse. It is both the
// ADK plugin and the span processor that links the call to the prompt version:
// ADK opens the generate_content span after the plugin returns, so the choice
// is kept per invocation until that span starts.
type Prompts struct {
	source PromptSource
	name   string

	choices sync.Map
}

var _ sdktrace.SpanProcessor = (*Prompts)(nil)

func NewPrompts(source PromptSource, promptName string) *Prompts {
	return &Prompts{source: source, name: promptName}
}

func (p *Prompts) Plugin() (*plugin.Plugin, error) {
	return plugin.New(plugin.Config{
		Name:                "langfuse_prompts",
		BeforeModelCallback: p.BeforeModel,
		AfterRunCallback:    func(ctx agent.InvocationContext) { p.choices.Delete(ctx.InvocationID()) },
	})
}

func (p *Prompts) choose(ctx context.Context, sessionID string) (choice, error) {
	production, err := p.source.Prompt(ctx, p.name, Production)
	if err != nil {
		return choice{}, err
	}
	label := VariantLabel(production, sessionID)
	if label == Production {
		return choice{production, label}, nil
	}
	variant, err := p.source.Prompt(ctx, p.name, label)
	if err != nil {
		return choice{}, err
	}
	return choice{variant, label}, nil
}

func (p *Prompts) BeforeModel(ctx agent.Context, req *model.LLMRequest) (*model.LLMResponse, error) {
	chosen, err := p.choose(ctx, ctx.SessionID())
	if err != nil {
		return nil, fmt.Errorf("langfuse prompt %s: %w", p.name, err)
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
			traceTagsAttribute.StringSlice([]string{"variant:" + chosen.label}),
		)
		return
	}
}

func (p *Prompts) OnEnd(sdktrace.ReadOnlySpan) {}

func (p *Prompts) Shutdown(context.Context) error { return nil }

func (p *Prompts) ForceFlush(context.Context) error { return nil }
