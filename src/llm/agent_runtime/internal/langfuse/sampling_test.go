package langfuse

import (
	"context"
	"encoding/json"
	"testing"

	kagentagent "github.com/kagent-dev/kagent/go/adk/pkg/agent"
	"github.com/kagent-dev/kagent/go/adk/pkg/models"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	"google.golang.org/adk/v2/model"
)

func ptr[T any](value T) *T { return &value }

func samplingFixture() (Sampling, *Prompts, Prompt, Prompt) {
	live := Sampling{Temperature: ptr(0.7), TopP: ptr(0.8), MaxTokens: ptr(int64(1024))}
	test := Config{ABTest: &ABTest{Label: "variant-b", Share: ptr(1.0)}}
	production := Prompt{Name: "evidence-agent-system-prompt", Version: 3, Prompt: "base", Config: test}
	variant := Prompt{
		Name: "evidence-agent-system-prompt", Version: 4, Prompt: "tuned",
		Config: Config{Model: &ModelSettings{Temperature: ptr(0.1), MaxTokens: ptr(int64(128))}},
	}
	prompts := NewPrompts(&fakeSource{prompts: map[string]Prompt{Production: production, "variant-b": variant}})
	prompts.Bind("evidence_agent", live)
	return live, prompts, production, variant
}

func TestAVariantsModelSettingsReplaceTheModelConfigsAndOthersKeepThem(t *testing.T) {
	live, prompts, _, _ := samplingFixture()

	if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if *live.Temperature != 0.1 || *live.MaxTokens != 128 || *live.TopP != 0.8 {
		t.Fatalf("temperature %v, topP %v, maxTokens %v", *live.Temperature, *live.TopP, *live.MaxTokens)
	}
}

func TestTheModelConfigsSettingsReturnWhenTheChosenVersionSetsNone(t *testing.T) {
	live := Sampling{Temperature: ptr(0.7), TopP: ptr(0.8), MaxTokens: ptr(int64(1024))}
	variantPrompt := Prompt{Name: "evidence-agent-system-prompt", Version: 4, Config: Config{Model: &ModelSettings{Temperature: ptr(0.1)}}}
	source := &fakeSource{prompts: map[string]Prompt{Production: variantPrompt}}
	prompts := NewPrompts(source)
	prompts.Bind("evidence_agent", live)
	call := func() {
		if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, &model.LLMRequest{}); err != nil {
			t.Fatal(err)
		}
	}

	call()
	if *live.Temperature != 0.1 {
		t.Fatalf("temperature %v", *live.Temperature)
	}
	source.prompts[Production] = Prompt{Name: "evidence-agent-system-prompt", Version: 5}
	call()
	if *live.Temperature != 0.7 || *live.TopP != 0.8 || *live.MaxTokens != 1024 {
		t.Fatalf("temperature %v, topP %v, maxTokens %v", *live.Temperature, *live.TopP, *live.MaxTokens)
	}
}

func TestEachAgentKeepsItsOwnBaselineAndUnboundAgentsAreUntouched(t *testing.T) {
	live, prompts, _, _ := samplingFixture()
	other := Sampling{Temperature: ptr(0.3)}
	prompts.Bind("risk_agent", other)

	if _, err := prompts.BeforeModel(callbackContext{agent: "evidence_agent", session: "s1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if *other.Temperature != 0.3 || *live.Temperature != 0.1 {
		t.Fatalf("other %v, evidence %v", *other.Temperature, *live.Temperature)
	}
	if _, err := prompts.BeforeModel(callbackContext{agent: "coordinator", session: "s1"}, &model.LLMRequest{}); err != nil {
		t.Fatal(err)
	}
	if *other.Temperature != 0.3 || *live.Temperature != 0.1 {
		t.Fatalf("an unbound agent changed other %v, evidence %v", *other.Temperature, *live.Temperature)
	}
}

func TestASettingTheModelConfigLeavesUnsetCannotBeOverridden(t *testing.T) {
	live := Sampling{Temperature: ptr(0.7)}
	newBaseline(live).apply(&ModelSettings{TopP: ptr(0.2), MaxTokens: ptr(int64(5))})
	if live.TopP != nil || live.MaxTokens != nil || *live.Temperature != 0.7 {
		t.Fatalf("%+v", live)
	}
}

func TestPromptConfigDecodesModelSettings(t *testing.T) {
	var prompt Prompt
	body := `{"name":"a","version":2,"prompt":"p","config":{"abTest":{"label":"variant-b"},"model":{"temperature":0.2,"topP":0.9,"maxTokens":256}}}`
	if err := json.Unmarshal([]byte(body), &prompt); err != nil {
		t.Fatal(err)
	}
	got := prompt.Config.Model
	if *got.Temperature != 0.2 || *got.TopP != 0.9 || *got.MaxTokens != 256 {
		t.Fatalf("%+v", got)
	}
	if prompt.Config.ABTest.Label != "variant-b" {
		t.Fatal("abTest lost")
	}
}

func TestKagentsOpenAIModelReadsItsSettingsThroughTheSamePointers(t *testing.T) {
	cfg := &configv1.ADKModelConfig{
		Model: "qwen",
		Provider: &configv1.ADKModelConfig_Openai{Openai: &configv1.ADKOpenAI{
			BaseUrl: "http://gateway/v1", Temperature: ptr(0.7), TopP: ptr(0.8), MaxTokens: ptr(int64(1024)),
		}},
	}
	llm, err := kagentagent.CreateLLM(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	openai := cfg.GetOpenai()
	newBaseline(Sampling{Temperature: openai.Temperature, TopP: openai.TopP, MaxTokens: openai.MaxTokens}).
		apply(&ModelSettings{Temperature: ptr(0.1), MaxTokens: ptr(int64(128))})

	used := llm.(*models.OpenAIModel).Config
	if *used.Temperature != 0.1 || *used.MaxTokens != 128 || *used.TopP != 0.8 {
		t.Fatalf("the model reads %v, %v, %v", *used.Temperature, *used.TopP, *used.MaxTokens)
	}
}
