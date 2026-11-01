// Package langfuse runs each model call from the agent's Langfuse prompt.
//
// Langfuse is the source of the system prompt and its A/B test. The production
// version of the agent's prompt may name a test in its config:
// {"abTest": {"label": "variant-b", "share": 0.5}}. Each conversation falls on
// one side by a hash of its session id, so it keeps its variant for every
// model call, and the call is linked to the chosen version in Langfuse.
package langfuse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const Production = "production"

type ABTest struct {
	Label string   `json:"label"`
	Share *float64 `json:"share"`
}

type Config struct {
	ABTest *ABTest `json:"abTest"`
}

type Prompt struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Prompt  string `json:"prompt"`
	Config  Config `json:"config"`
}

// VariantLabel picks the label of the prompt version a conversation uses.
func VariantLabel(production Prompt, sessionID string) string {
	test := production.Config.ABTest
	if test == nil || test.Label == "" {
		return Production
	}
	share := 0.5
	if test.Share != nil {
		share = *test.Share
	}
	sum := sha256.Sum256([]byte(production.Name + ":" + sessionID))
	bucket := float64(binary.BigEndian.Uint32(sum[:4])) / float64(^uint32(0))
	if bucket < share {
		return test.Label
	}
	return Production
}

type cached struct {
	prompt  Prompt
	fetched time.Time
}

// Client reads text prompts from the Langfuse public API. Credentials are not
// held here: the sandbox egress gateway replaces the Authorization header.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	TTL     time.Duration
	Now     func() time.Time

	mu    sync.Mutex
	cache map[string]cached
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
		TTL:     time.Minute,
		Now:     time.Now,
		cache:   map[string]cached{},
	}
}

// Prompt returns the labelled version, from cache within the TTL. When Langfuse
// cannot be reached, the last version fetched is served instead.
func (c *Client) Prompt(ctx context.Context, name, label string) (Prompt, error) {
	key := name + "@" + label
	c.mu.Lock()
	entry, ok := c.cache[key]
	c.mu.Unlock()
	if ok && c.Now().Sub(entry.fetched) < c.TTL {
		return entry.prompt, nil
	}
	prompt, err := c.fetch(ctx, name, label)
	if err != nil {
		if ok {
			return entry.prompt, nil
		}
		return Prompt{}, err
	}
	c.mu.Lock()
	c.cache[key] = cached{prompt: prompt, fetched: c.Now()}
	c.mu.Unlock()
	return prompt, nil
}

func (c *Client) fetch(ctx context.Context, name, label string) (Prompt, error) {
	endpoint := fmt.Sprintf("%s/api/public/v2/prompts/%s?label=%s", c.BaseURL, url.PathEscape(name), url.QueryEscape(label))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Prompt{}, err
	}
	req.Header.Set("Authorization", "Basic placeholder")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Prompt{}, fmt.Errorf("fetch prompt %s@%s: %w", name, label, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Prompt{}, fmt.Errorf("fetch prompt %s@%s: %s", name, label, resp.Status)
	}
	var prompt Prompt
	if err := json.NewDecoder(resp.Body).Decode(&prompt); err != nil {
		return Prompt{}, fmt.Errorf("decode prompt %s@%s: %w", name, label, err)
	}
	return prompt, nil
}
