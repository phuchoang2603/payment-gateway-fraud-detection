package remoteagents

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
)

type recorder struct {
	bodies []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	body := ""
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		body = string(raw)
		if int64(len(raw)) != req.ContentLength {
			return nil, io.ErrShortBuffer
		}
	}
	r.bodies = append(r.bodies, body)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

var agents = []*configv1.ADKRemoteAgentConfig{
	{Name: "evidence_agent", Url: "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent/.well-known/agent-card.json"},
}

func send(t *testing.T, transport http.RoundTripper, method, url, body string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
}

func TestTheCallersTenantIsDroppedFromDelegatedCalls(t *testing.T) {
	base := &recorder{}
	transport := WithoutTenant(base, agents)

	send(t, transport, http.MethodPost, "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent",
		`{"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":{"tenant":"llm-agents/coordinator","message":{"messageId":"m1"}}}`)

	var call struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal([]byte(base.bodies[0]), &call); err != nil {
		t.Fatal(err)
	}
	if _, ok := call.Params["tenant"]; ok {
		t.Fatalf("tenant kept: %s", base.bodies[0])
	}
	if call.Method != "SendStreamingMessage" || string(call.Params["message"]) != `{"messageId":"m1"}` {
		t.Fatalf("call changed: %s", base.bodies[0])
	}
}

func TestOtherRequestsAreUntouched(t *testing.T) {
	withTenant := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"tenant":"t"}}`
	for _, tc := range []struct{ method, url, body string }{
		{http.MethodPost, "http://agentgateway.llm.svc.cluster.local/v1/chat/completions", withTenant},
		{http.MethodPost, "http://agentgateway.llm.svc.cluster.local/a2a/risk-agent", withTenant},
		{http.MethodGet, "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent", ""},
		{http.MethodPost, "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent/", `{"jsonrpc":"2.0","params":{"message":{}}}`},
		{http.MethodPost, "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent", `not json`},
		{http.MethodPost, "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent", `{"params":"x"}`},
	} {
		base := &recorder{}
		send(t, WithoutTenant(base, agents), tc.method, tc.url, tc.body)
		if base.bodies[0] != tc.body {
			t.Fatalf("%s %s: body changed to %s", tc.method, tc.url, base.bodies[0])
		}
	}
}
