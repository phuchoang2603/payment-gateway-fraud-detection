package integrations_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/integrations"
)

func digest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func TestKeysResolveOnlyThroughTheirHash(t *testing.T) {
	raw := fmt.Sprintf(`[
	{"integration_id":"int-a","name":"a","api_key_sha256":%q,"synthetic":true,"webhook":{"url":"https://hook","secret":"s","events":["payment.succeeded"]}},
	{"integration_id":"int-b","name":"b","api_key_sha256":%q,"synthetic":false}]`, digest("key-a"), digest("key-b"))
	registry, err := integrations.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	a, ok := registry.Authenticate("key-a")
	if !ok || a.ID != "int-a" || !a.Synthetic {
		t.Fatalf("key-a = %+v, %v", a, ok)
	}
	b, ok := registry.Authenticate("key-b")
	if !ok || b.ID != "int-b" || b.Synthetic {
		t.Fatalf("key-b = %+v, %v", b, ok)
	}
	for _, unknown := range []string{"", "key-c", digest("key-a"), "KEY-A", "key-a "} {
		if _, ok := registry.Authenticate(unknown); ok {
			t.Errorf("unknown key %q was accepted", unknown)
		}
	}
	if found, ok := registry.ByID("int-b"); !ok || found.Name != "b" {
		t.Fatalf("ByID = %+v, %v", found, ok)
	}
	if _, ok := registry.ByID("int-z"); ok {
		t.Fatal("unknown integration resolved")
	}
}

func TestInvalidIntegrationsAreRejected(t *testing.T) {
	good := digest("k")
	tests := map[string]string{
		"empty list":     `[]`,
		"not json":       `{`,
		"no id":          fmt.Sprintf(`[{"api_key_sha256":%q}]`, good),
		"plaintext key":  `[{"integration_id":"a","api_key_sha256":"my-plain-key"}]`,
		"short hash":     `[{"integration_id":"a","api_key_sha256":"abcd"}]`,
		"duplicate id":   fmt.Sprintf(`[{"integration_id":"a","api_key_sha256":%q},{"integration_id":"a","api_key_sha256":%q}]`, good, digest("j")),
		"duplicate hash": fmt.Sprintf(`[{"integration_id":"a","api_key_sha256":%q},{"integration_id":"b","api_key_sha256":%q}]`, good, strings.ToUpper(good)),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := integrations.Parse([]byte(raw)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
