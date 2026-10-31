package remoteagents

import (
	"maps"
	"testing"
)

func TestRemoteAgentsBecomeIsolatedA2AToolsWithAPlaceholderKey(t *testing.T) {
	agents, err := Parse(`[{"name":"evidence_agent","description":"Explains declines.",
		"url":"http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent/"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 {
		t.Fatalf("got %d agents", len(agents))
	}
	agent := agents[0]
	if agent.Name != "evidence_agent" || agent.Description != "Explains declines." {
		t.Fatalf("got %+v", agent)
	}
	if agent.Url != "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent" {
		t.Fatalf("url %q", agent.Url)
	}
	if !maps.Equal(agent.Headers, map[string]string{"Authorization": "placeholder"}) {
		t.Fatalf("headers %v", agent.Headers)
	}
	if !agent.IsolateSessions {
		t.Fatal("sub-agent sessions should be isolated")
	}
}

func TestNoRemoteAgentsIsEmpty(t *testing.T) {
	for _, raw := range []string{"", "  "} {
		agents, err := Parse(raw)
		if err != nil || agents != nil {
			t.Fatalf("%q: got %v, %v", raw, agents, err)
		}
	}
}

func TestInvalidRemoteAgentsAreRejected(t *testing.T) {
	for _, raw := range []string{"{", `[{"name":"a"}]`, `[{"url":"http://x"}]`} {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("%q: expected an error", raw)
		}
	}
}
