package langfuse

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/genai"
)

type fakeSource struct {
	prompts map[string]Prompt
	err     error
	labels  []string
}

func (f *fakeSource) Prompt(_ context.Context, name, label string) (Prompt, error) {
	f.labels = append(f.labels, name+"@"+label)
	if f.err != nil {
		return Prompt{}, f.err
	}
	prompt, ok := f.prompts[label]
	if !ok {
		return Prompt{}, errors.New("no such label")
	}
	return prompt, nil
}

type callbackContext struct {
	agent.Context
	agent, session, invocation string
}

func (c callbackContext) AgentName() string    { return c.agent }
func (c callbackContext) SessionID() string    { return c.session }
func (c callbackContext) InvocationID() string { return c.invocation }

type invocationContext struct {
	agent.InvocationContext
	invocation string
}

func (c invocationContext) InvocationID() string { return c.invocation }

var (
	production = Prompt{Name: "evidence-agent-system-prompt", Version: 3, Prompt: "You are read-only."}
	tested     = Prompt{
		Name: "evidence-agent-system-prompt", Version: 3, Prompt: "You are read-only.",
		Config: Config{ABTest: &ABTest{Label: "variant-b", Share: share(1)}},
	}
	variant = Prompt{Name: "evidence-agent-system-prompt", Version: 4, Prompt: "You are read-only, briefly."}
)

func instructionText(req *model.LLMRequest) []string {
	var texts []string
	for _, part := range req.Config.SystemInstruction.Parts {
		texts = append(texts, part.Text)
	}
	return texts
}

func TestTheProductionPromptBecomesTheSystemInstruction(t *testing.T) {
	source := &fakeSource{prompts: map[string]Prompt{Production: production}}
	prompts := NewPrompts(source)
	req := &model.LLMRequest{}

	resp, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1", invocation: "i1"}, req)
	if resp != nil || err != nil {
		t.Fatalf("got %v, %v; the model call should go ahead", resp, err)
	}
	if got := instructionText(req); !slices.Equal(got, []string{"You are read-only."}) {
		t.Fatalf("instruction %q", got)
	}
	if !slices.Equal(source.labels, []string{"evidence-agent-system-prompt@production"}) {
		t.Fatalf("fetched %v", source.labels)
	}
}

func TestThePromptComesBeforeKagentsOwnGuidance(t *testing.T) {
	prompts := NewPrompts(&fakeSource{prompts: map[string]Prompt{Production: production}})
	req := &model.LLMRequest{Config: &genai.GenerateContentConfig{
		SystemInstruction: genai.NewContentFromText("Use load_skill before following a skill.", genai.RoleUser),
	}}

	if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, req); err != nil {
		t.Fatal(err)
	}
	want := []string{"You are read-only.", "Use load_skill before following a skill."}
	if got := instructionText(req); !slices.Equal(got, want) {
		t.Fatalf("instruction %q, want %q", got, want)
	}
}

func TestTheVariantVersionIsUsedForItsConversations(t *testing.T) {
	source := &fakeSource{prompts: map[string]Prompt{Production: tested, "variant-b": variant}}
	prompts := NewPrompts(source)
	req := &model.LLMRequest{}

	if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, req); err != nil {
		t.Fatal(err)
	}
	if got := instructionText(req); !slices.Equal(got, []string{"You are read-only, briefly."}) {
		t.Fatalf("instruction %q", got)
	}
	if want := []string{"evidence-agent-system-prompt@production", "evidence-agent-system-prompt@variant-b"}; !slices.Equal(source.labels, want) {
		t.Fatalf("fetched %v", source.labels)
	}
}

func TestAMissingPromptFailsTheModelCall(t *testing.T) {
	for _, source := range []*fakeSource{
		{err: errors.New("langfuse down")},
		{prompts: map[string]Prompt{Production: tested}},
	} {
		req := &model.LLMRequest{}
		if _, err := NewPrompts(source).BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, req); err == nil {
			t.Fatalf("%+v: expected an error", source)
		}
		if req.Config != nil {
			t.Fatal("the request changed although the prompt failed")
		}
	}
}

func recordSpans(t *testing.T, prompts *Prompts) (trace.Tracer, *tracetest.SpanRecorder) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(prompts), sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider.Tracer("test"), recorder
}

func attributes(span sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	values := map[attribute.Key]attribute.Value{}
	for _, kv := range span.Attributes() {
		values[kv.Key] = kv.Value
	}
	return values
}

func TestTheModelCallSpanIsLinkedToTheChosenVersion(t *testing.T) {
	prompts := NewPrompts(&fakeSource{prompts: map[string]Prompt{Production: tested, "variant-b": variant}})
	tracer, recorder := recordSpans(t, prompts)
	if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1", invocation: "i1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}

	_, span := tracer.Start(context.Background(), "generate_content gpt-oss", trace.WithAttributes(invocationIDAttribute.String("i1")))
	span.End()
	_, other := tracer.Start(context.Background(), "generate_content gpt-oss", trace.WithAttributes(invocationIDAttribute.String("i2")))
	other.End()
	_, agentSpan := tracer.Start(context.Background(), "invoke_agent evidence-agent", trace.WithAttributes(invocationIDAttribute.String("i1")))
	agentSpan.End()

	ended := recorder.Ended()
	linked := attributes(ended[0])
	if linked[promptNameAttribute].AsString() != "evidence-agent-system-prompt" || linked[promptVersionAttribute].AsInt64() != 4 {
		t.Fatalf("prompt link %v", linked)
	}
	if got := linked[traceTagsAttribute].AsStringSlice(); !slices.Equal(got, []string{"evidence-agent:variant-b"}) {
		t.Fatalf("tags %v", got)
	}
	for _, span := range ended[1:] {
		if _, ok := attributes(span)[promptNameAttribute]; ok {
			t.Fatalf("%s was linked", span.Name())
		}
	}
}

func TestTheChoiceIsDroppedWhenTheRunEnds(t *testing.T) {
	prompts := NewPrompts(&fakeSource{prompts: map[string]Prompt{Production: production}})
	tracer, recorder := recordSpans(t, prompts)
	plugin, err := prompts.Plugin()
	if err != nil {
		t.Fatal(err)
	}
	if plugin.Name() != "langfuse_prompts" {
		t.Fatalf("name %q", plugin.Name())
	}
	if _, err := plugin.BeforeModelCallback()(callbackContext{agent: "evidence_agent", session: "s1", invocation: "i1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	plugin.AfterRunCallback()(invocationContext{invocation: "i1"})

	_, span := tracer.Start(context.Background(), "generate_content gpt-oss", trace.WithAttributes(invocationIDAttribute.String("i1")))
	span.End()
	if _, ok := attributes(recorder.Ended()[0])[promptNameAttribute]; ok {
		t.Fatal("a finished run's choice was still linked")
	}
	if prompts.Shutdown(context.Background()) != nil || prompts.ForceFlush(context.Background()) != nil {
		t.Fatal("the processor holds nothing to flush")
	}
}

func TestEachAgentInTheRuntimeUsesItsOwnPrompt(t *testing.T) {
	source := &fakeSource{prompts: map[string]Prompt{Production: production}}
	prompts := NewPrompts(source)
	for _, agentName := range []string{"coordinator", "risk_agent"} {
		if _, err := prompts.BeforeModel(callbackContext{agent: agentName, session: "s1"}, &model.LLMRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"coordinator-system-prompt@production", "risk-agent-system-prompt@production"}
	if !slices.Equal(source.labels, want) {
		t.Fatalf("fetched %v, want %v", source.labels, want)
	}
}

func TestAnAgentWithoutATestLinksItsPromptButAddsNoTag(t *testing.T) {
	prompts := NewPrompts(&fakeSource{prompts: map[string]Prompt{Production: production}})
	tracer, recorder := recordSpans(t, prompts)
	if _, err := prompts.BeforeModel(callbackContext{agent: "risk_agent", session: "s1", invocation: "i1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}

	_, span := tracer.Start(context.Background(), "generate_content gpt-oss", trace.WithAttributes(invocationIDAttribute.String("i1")))
	span.End()

	linked := attributes(recorder.Ended()[0])
	if linked[promptVersionAttribute].AsInt64() != 3 {
		t.Fatalf("prompt link %v", linked)
	}
	if _, ok := linked[traceTagsAttribute]; ok {
		t.Fatalf("tagged without a test: %v", linked[traceTagsAttribute])
	}
}
