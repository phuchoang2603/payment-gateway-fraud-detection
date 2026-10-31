package remoteagents

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
)

const cardPath = "/.well-known/agent-card.json"

// WithoutTenant drops the A2A tenant from JSON-RPC calls to the remote agents'
// endpoints. kagent's remote agent tool lets a2a-go copy the calling agent's
// own tenant onto the call, and kagent rejects a tenant that differs from the
// agent in the URL ("tenant does not match the Agent URL"). Without one, kagent
// takes the agent from the URL, as for any other client.
func WithoutTenant(base http.RoundTripper, agents []*configv1.ADKRemoteAgentConfig) http.RoundTripper {
	endpoints := make([]string, 0, len(agents))
	for _, agent := range agents {
		endpoints = append(endpoints, strings.TrimSuffix(agent.Url, cardPath))
	}
	return tenantStripper{base: base, endpoints: endpoints}
}

type tenantStripper struct {
	base      http.RoundTripper
	endpoints []string
}

func (t tenantStripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.Body == nil || !t.remote(req) {
		return t.base.RoundTrip(req)
	}
	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, err
	}
	body = withoutTenant(body)
	out := req.Clone(req.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return t.base.RoundTrip(out)
}

func (t tenantStripper) remote(req *http.Request) bool {
	url := strings.TrimSuffix(req.URL.Scheme+"://"+req.URL.Host+req.URL.Path, "/")
	for _, endpoint := range t.endpoints {
		if url == endpoint {
			return true
		}
	}
	return false
}

// withoutTenant returns the JSON-RPC body without params.tenant, or the body
// unchanged when it is not a JSON-RPC request with one.
func withoutTenant(body []byte) []byte {
	var call map[string]json.RawMessage
	if json.Unmarshal(body, &call) != nil {
		return body
	}
	var params map[string]json.RawMessage
	if json.Unmarshal(call["params"], &params) != nil {
		return body
	}
	if _, ok := params["tenant"]; !ok {
		return body
	}
	delete(params, "tenant")
	encoded, err := json.Marshal(params)
	if err != nil {
		return body
	}
	call["params"] = encoded
	out, err := json.Marshal(call)
	if err != nil {
		return body
	}
	return out
}
