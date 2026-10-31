// Package remoteagents turns REMOTE_AGENTS into A2A tools that call other
// agents through the A2A gateway.
package remoteagents

import (
	"encoding/json"
	"fmt"
	"strings"

	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
)

type remoteAgent struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	URL         string `json:"url"`
}

// Parse reads the JSON list. The Authorization value is a placeholder that the
// sandbox egress gateway replaces with the gateway key.
func Parse(raw string) ([]*configv1.ADKRemoteAgentConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var agents []remoteAgent
	if err := json.Unmarshal([]byte(raw), &agents); err != nil {
		return nil, fmt.Errorf("parse REMOTE_AGENTS: %w", err)
	}
	configs := make([]*configv1.ADKRemoteAgentConfig, 0, len(agents))
	for _, a := range agents {
		if a.Name == "" || a.URL == "" {
			return nil, fmt.Errorf("parse REMOTE_AGENTS: each agent needs a name and url")
		}
		configs = append(configs, &configv1.ADKRemoteAgentConfig{
			Name:            a.Name,
			Description:     a.Description,
			Url:             strings.TrimRight(a.URL, "/"),
			Headers:         map[string]string{"Authorization": "placeholder"},
			IsolateSessions: true,
		})
	}
	return configs, nil
}
