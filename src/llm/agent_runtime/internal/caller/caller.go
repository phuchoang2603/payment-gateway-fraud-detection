package caller

import (
	"context"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/kagent-dev/kagent/go/adk/pkg/auth"
	"github.com/kagent-dev/kagent/go/adk/pkg/models"
	controllerauth "github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type Identity struct {
	Client      string
	Integration string
}

type Issuers struct {
	Gateway       string `json:"gateway"`
	Analyst       string `json:"analyst"`
	AnalystClient string `json:"analystClient"`
	UserIDClaim   string `json:"userIdClaim"`
}

func (i Issuers) userIDClaim() string {
	if i.UserIDClaim == "" {
		return "sub"
	}
	return i.UserIDClaim
}

func FromContext(ctx context.Context, issuers Issuers) (Identity, error) {
	raw := models.BearerFromCallContext(ctx)
	principal := auth.UserIDFromContext(ctx)
	if raw == "" || principal == "" {
		return Identity{}, fmt.Errorf("authenticated caller required")
	}
	claims := jwt.MapClaims{}
	// NOTE: parsing unverified is safe only because the controller admits waypoint-verified credentials; claims count only when their issuer and session owner match its authenticated principal.
	if _, _, err := jwt.NewParser().ParseUnverified(raw, claims); err != nil {
		return Identity{}, fmt.Errorf("invalid caller credential")
	}
	if err := jwt.NewValidator(jwt.WithExpirationRequired()).Validate(claims); err != nil {
		return Identity{}, fmt.Errorf("expired or invalid caller credential")
	}
	issuer, _ := claims.GetIssuer()
	subject, _ := claims.GetSubject()
	owner, _ := claims[issuers.userIDClaim()].(string)
	if owner == "" {
		owner = subject
	}
	if owner == "" || principal != controllerauth.PrincipalID("trusted-proxy:"+issuer, owner) {
		return Identity{}, fmt.Errorf("caller principal mismatch")
	}
	identity := Identity{}
	switch {
	case issuers.Gateway != "" && issuer == issuers.Gateway:
		if err := jwt.NewValidator(jwt.WithAudience("kagent")).Validate(claims); err != nil {
			return Identity{}, fmt.Errorf("invalid caller audience")
		}
		identity.Client, _ = claims["client"].(string)
		identity.Integration, _ = claims["integration_id"].(string)
		if identity.Client == "" {
			return Identity{}, fmt.Errorf("invalid caller identity")
		}
	case issuers.Analyst != "" && issuers.AnalystClient != "" && issuer == issuers.Analyst:
		identity.Client = issuers.AnalystClient
	default:
		return Identity{}, fmt.Errorf("unknown caller issuer")
	}
	return identity, nil
}
