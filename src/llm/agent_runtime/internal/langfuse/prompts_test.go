package langfuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func share(v float64) *float64 { return &v }

func TestAShareOfZeroOrOneSendsEveryConversationToOneSide(t *testing.T) {
	for _, tc := range []struct {
		share float64
		want  string
	}{{0, Production}, {1, "variant-b"}} {
		prompt := Prompt{Name: "p", Config: Config{ABTest: &ABTest{Label: "variant-b", Share: share(tc.share)}}}
		for i := range 50 {
			if got := VariantLabel(prompt, fmt.Sprintf("session-%d", i)); got != tc.want {
				t.Fatalf("share %v: session-%d got %q, want %q", tc.share, i, got, tc.want)
			}
		}
	}
}

func TestAConversationKeepsItsVariantAndTheSplitFollowsTheShare(t *testing.T) {
	prompt := Prompt{Name: "evidence-agent-system-prompt", Config: Config{ABTest: &ABTest{Label: "variant-b"}}}
	variants := 0
	for i := range 2000 {
		session := fmt.Sprintf("session-%d", i)
		label := VariantLabel(prompt, session)
		if label != VariantLabel(prompt, session) {
			t.Fatalf("%s changed variant", session)
		}
		if label == "variant-b" {
			variants++
		}
	}
	if ratio := float64(variants) / 2000; ratio < 0.45 || ratio > 0.55 {
		t.Fatalf("variant share %.3f, want about 0.5", ratio)
	}
}

func TestWithoutANamedVariantEveryConversationUsesProduction(t *testing.T) {
	for _, config := range []Config{{}, {ABTest: &ABTest{}}, {ABTest: &ABTest{Share: share(1)}}} {
		if got := VariantLabel(Prompt{Name: "p", Config: config}, "s"); got != Production {
			t.Fatalf("%+v: got %q", config, got)
		}
	}
}

type langfuseServer struct {
	*httptest.Server
	calls  atomic.Int32
	failed atomic.Bool
}

func newLangfuseServer(t *testing.T) *langfuseServer {
	s := &langfuseServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		if s.failed.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		if r.URL.Path != "/api/public/v2/prompts/evidence-agent-system-prompt" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Basic placeholder" {
			t.Errorf("Authorization %q, want the egress placeholder", got)
		}
		_, _ = fmt.Fprintf(w, `{"name":"evidence-agent-system-prompt","version":%d,"prompt":"You are read-only.","type":"text",
			"config":{"abTest":{"label":"variant-b","share":0.25}},"labels":[%q]}`, s.calls.Load(), r.URL.Query().Get("label"))
	}))
	t.Cleanup(s.Close)
	return s
}

func TestTheClientReadsTheLabelledVersion(t *testing.T) {
	server := newLangfuseServer(t)
	client := NewClient(server.URL + "/")

	prompt, err := client.Prompt(context.Background(), "evidence-agent-system-prompt", Production)
	if err != nil {
		t.Fatal(err)
	}
	if prompt.Name != "evidence-agent-system-prompt" || prompt.Version != 1 || prompt.Prompt != "You are read-only." {
		t.Fatalf("got %+v", prompt)
	}
	if test := prompt.Config.ABTest; test == nil || test.Label != "variant-b" || *test.Share != 0.25 {
		t.Fatalf("abTest %+v", prompt.Config.ABTest)
	}
}

func TestTheClientCachesForTheTTLAndServesTheLastVersionWhenLangfuseIsDown(t *testing.T) {
	server := newLangfuseServer(t)
	client := NewClient(server.URL)
	now := time.Unix(0, 0)
	client.Now = func() time.Time { return now }
	get := func() Prompt {
		t.Helper()
		prompt, err := client.Prompt(context.Background(), "evidence-agent-system-prompt", Production)
		if err != nil {
			t.Fatal(err)
		}
		return prompt
	}

	get()
	now = now.Add(59 * time.Second)
	if get().Version != 1 || server.calls.Load() != 1 {
		t.Fatalf("refetched within the TTL: %d calls", server.calls.Load())
	}
	now = now.Add(time.Second)
	if get().Version != 2 {
		t.Fatal("did not refetch after the TTL")
	}
	server.failed.Store(true)
	now = now.Add(time.Minute)
	if get().Version != 2 || server.calls.Load() != 3 {
		t.Fatalf("expected the cached version after a failed refetch, %d calls", server.calls.Load())
	}
}

func TestTheClientFailsWithoutACachedVersion(t *testing.T) {
	server := newLangfuseServer(t)
	client := NewClient(server.URL)
	if _, err := client.Prompt(context.Background(), "missing", Production); err == nil {
		t.Fatal("expected an error for a missing prompt")
	}
	server.failed.Store(true)
	if _, err := client.Prompt(context.Background(), "evidence-agent-system-prompt", Production); err == nil {
		t.Fatal("expected an error when Langfuse is down")
	}
	client.BaseURL = "http://127.0.0.1:1"
	if _, err := client.Prompt(context.Background(), "evidence-agent-system-prompt", "variant-b"); err == nil {
		t.Fatal("expected an error when Langfuse is unreachable")
	}
}

func TestTheClientRejectsAChatPrompt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"name":"p","version":1,"prompt":[{"role":"system","content":"x"}]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := NewClient(server.URL).Prompt(context.Background(), "p", Production); err == nil {
		t.Fatal("expected a decode error")
	}
}
