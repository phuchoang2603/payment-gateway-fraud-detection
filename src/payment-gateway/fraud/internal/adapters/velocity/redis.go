package velocity

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	"github.com/redis/go-redis/v9"
)

//go:embed register.lua
var registrationScript string

type Config struct {
	Address, Password, Epoch, PolicyVersion string
	RetryHorizon, Timeout                   time.Duration
	Threshold10s, Threshold60s              int64
}

type Guard struct {
	client *redis.Client
	script *redis.Script
	config Config
}

func New(config Config) (*Guard, error) {
	if config.Address == "" || config.Password == "" || config.Epoch == "" || config.PolicyVersion == "" || config.RetryHorizon < time.Minute || config.Timeout <= 0 || config.Threshold10s <= 0 || config.Threshold60s <= 0 {
		return nil, errors.New("invalid authorization Redis configuration")
	}
	client := redis.NewClient(&redis.Options{
		Addr: config.Address, Password: config.Password, DialTimeout: config.Timeout,
		ReadTimeout: config.Timeout, WriteTimeout: config.Timeout, PoolTimeout: config.Timeout,
		ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 32, DisableIdentity: true,
	})
	return &Guard{client: client, script: redis.NewScript(registrationScript), config: config}, nil
}

func (g *Guard) Close() error { return g.client.Close() }

func hash(value ...string) string {
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (g *Guard) Register(ctx context.Context, request application.Request, first time.Time) (application.GuardObservation, application.GuardObservation) {
	method := make(chan application.GuardObservation, 1)
	go func() { method <- g.register(ctx, request, first, "method", request.MethodFingerprint) }()
	customer := g.register(ctx, request, first, "customer", request.CustomerID)
	return customer, <-method
}

func (g *Guard) register(ctx context.Context, request application.Request, first time.Time, view, entity string) application.GuardObservation {
	ctx, cancel := context.WithTimeout(ctx, g.config.Timeout)
	defer cancel()
	observation := application.GuardObservation{Status: "error", PolicyVersion: g.config.PolicyVersion, Threshold10s: g.config.Threshold10s, Threshold60s: g.config.Threshold60s}
	prefix := g.prefix(request.IntegrationID, view, entity)
	member := hash(request.IntegrationID, request.PaymentID, request.AttemptID)
	values, err := g.script.Run(ctx, g.client, []string{prefix + "window", prefix + "coverage", prefix + member}, member, request.Digest, first.UnixMilli(), g.config.RetryHorizon.Milliseconds(), g.config.Epoch).Slice()
	if err != nil {
		var timeout net.Error
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			observation.Status = "timeout"
		}
		return observation
	}
	if len(values) != 6 {
		return observation
	}
	status, ok := values[0].(string)
	if !ok {
		return observation
	}
	epoch, ok := values[1].(string)
	if !ok {
		return observation
	}
	numbers := [4]int64{}
	for i := range numbers {
		switch value := values[i+2].(type) {
		case int64:
			numbers[i] = value
		case string:
			numbers[i], err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return observation
			}
		default:
			return observation
		}
	}
	observation.Status, observation.Epoch = status, epoch
	read := time.UnixMilli(numbers[1]).UTC()
	observation.ReadAt = &read
	if status == "registered" || status == "coverage_unknown" {
		registered := time.UnixMilli(numbers[0]).UTC()
		observation.RegisteredAt = &registered
		observation.Count10s, observation.Count60s = &numbers[2], &numbers[3]
		observation.ThresholdExceeded = numbers[2] > g.config.Threshold10s || numbers[3] > g.config.Threshold60s
	}
	return observation
}

func (g *Guard) prefix(integration, view, entity string) string {
	return "fraud:velocity:{" + hash(g.config.Epoch, integration, view, entity) + "}:"
}
