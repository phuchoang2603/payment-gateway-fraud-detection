package integrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
)

type entry struct {
	IntegrationID string `json:"integration_id"`
	Name          string `json:"name"`
	APIKeySHA256  string `json:"api_key_sha256"`
	Synthetic     bool   `json:"synthetic"`
}

type Registry struct {
	byKeyHash map[[sha256.Size]byte]domain.Integration
	byID      map[string]domain.Integration
}

// Parse reads only the identity and credential fields; webhook settings belong to the notifier.
func Parse(raw []byte) (*Registry, error) {
	var entries []entry
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&entries); err != nil {
		return nil, fmt.Errorf("parse integrations: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("integrations must list at least one integration")
	}
	registry := &Registry{
		byKeyHash: make(map[[sha256.Size]byte]domain.Integration, len(entries)),
		byID:      make(map[string]domain.Integration, len(entries)),
	}
	for index, item := range entries {
		if item.IntegrationID == "" {
			return nil, fmt.Errorf("integration %d: integration_id is required", index)
		}
		decoded, err := hex.DecodeString(item.APIKeySHA256)
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("integration %s: api_key_sha256 must be a hex SHA-256 digest", item.IntegrationID)
		}
		var digest [sha256.Size]byte
		copy(digest[:], decoded)
		integration := domain.Integration{ID: item.IntegrationID, Name: item.Name, Synthetic: item.Synthetic}
		if _, duplicate := registry.byID[item.IntegrationID]; duplicate {
			return nil, fmt.Errorf("integration %s is listed twice", item.IntegrationID)
		}
		if _, duplicate := registry.byKeyHash[digest]; duplicate {
			return nil, fmt.Errorf("integration %s reuses another integration's api key", item.IntegrationID)
		}
		registry.byID[item.IntegrationID] = integration
		registry.byKeyHash[digest] = integration
	}
	return registry, nil
}

func (r *Registry) Authenticate(apiKey string) (domain.Integration, bool) {
	if apiKey == "" {
		return domain.Integration{}, false
	}
	integration, found := r.byKeyHash[sha256.Sum256([]byte(apiKey))]
	return integration, found
}

func (r *Registry) ByID(id string) (domain.Integration, bool) {
	integration, found := r.byID[id]
	return integration, found
}
