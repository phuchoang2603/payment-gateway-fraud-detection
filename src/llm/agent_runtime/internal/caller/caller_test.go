package caller

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/golang-jwt/jwt/v5"
	"github.com/kagent-dev/kagent/go/adk/pkg/auth"
	controllerauth "github.com/kagent-dev/kagent/go/core/pkg/auth"
)

var gateway = Issuers{Gateway: "gateway", Analyst: "dex", AnalystClient: "analyst-ui", UserIDClaim: "user_id"}

func credentialContext(t *testing.T, issuer, owner string, claims jwt.MapClaims) context.Context {
	t.Helper()
	claims["iss"] = issuer
	claims["exp"] = time.Now().Add(time.Minute).Unix()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("test-signing-key"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := a2asrv.NewCallContext(context.Background(), a2asrv.NewServiceParams(map[string][]string{
		"authorization": {"Bearer " + token}, "x-fraud-caller": {"analyst-ui"}, "x-fraud-integration": {"forged"},
	}))
	return auth.WithUserID(ctx, controllerauth.PrincipalID("trusted-proxy:"+issuer, owner))
}

func gatewayClaims(client string) jwt.MapClaims {
	return jwt.MapClaims{"sub": "agentgateway", "aud": "kagent", "client": client, "integration_id": "integration-a", "user_id": client + ":integration-a"}
}

func TestGatewayCallerComesFromClaimsNotHeaders(t *testing.T) {
	ctx := credentialContext(t, "gateway", "caller-a:integration-a", gatewayClaims("caller-a"))
	identity, err := FromContext(ctx, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Client != "caller-a" || identity.Integration != "integration-a" {
		t.Fatalf("identity = %+v", identity)
	}
}

func TestRefuseMissingOrMismatchedNativeIdentity(t *testing.T) {
	valid := credentialContext(t, "gateway", "caller-a:integration-a", gatewayClaims("caller-a"))
	noClient := gatewayClaims("")
	wrongAudience := gatewayClaims("caller-a")
	wrongAudience["aud"] = "other"
	for name, candidate := range map[string]context.Context{
		"no credential":         context.Background(),
		"forged principal":      auth.WithUserID(valid, "forged"),
		"principal from sub":    credentialContext(t, "gateway", "agentgateway", gatewayClaims("caller-a")),
		"another caller's user": credentialContext(t, "gateway", "caller-b:integration-a", gatewayClaims("caller-a")),
		"no client claim":       credentialContext(t, "gateway", "agentgateway", noClient),
		"wrong audience":        credentialContext(t, "gateway", "caller-a:integration-a", wrongAudience),
		"untrusted issuer":      credentialContext(t, "untrusted", "caller-a:integration-a", gatewayClaims("caller-a")),
	} {
		if _, err := FromContext(candidate, gateway); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestNativeAnalystCredentialFallsBackToSubject(t *testing.T) {
	ctx := credentialContext(t, "dex", "operator", jwt.MapClaims{"sub": "operator", "aud": "kagent"})
	identity, err := FromContext(ctx, gateway)
	if err != nil || identity.Client != "analyst-ui" || identity.Integration != "" {
		t.Fatalf("identity = %+v, error = %v", identity, err)
	}
}
