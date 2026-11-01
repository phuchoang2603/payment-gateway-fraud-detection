package domain

import (
	"bytes"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestSaleHashSeparatesFieldsSoShiftedBoundariesDiffer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		sale := Sale{
			IntegrationID:   rapid.StringN(1, 16, -1).Draw(t, "integration"),
			Token:           rapid.String().Draw(t, "token"),
			Currency:        rapid.SampledFrom([]string{"CAD", "USD"}).Draw(t, "currency"),
			AmountMinorUnit: rapid.Int64Range(1, 1_000_000).Draw(t, "amount"),
		}
		shifted := sale
		last := len(sale.IntegrationID) - 1
		shifted.IntegrationID, shifted.Token = sale.IntegrationID[:last], sale.IntegrationID[last:]+sale.Token
		repriced := sale
		repriced.AmountMinorUnit++

		if !bytes.Equal(sale.Hash(), sale.Hash()) || bytes.Equal(sale.Hash(), shifted.Hash()) || bytes.Equal(sale.Hash(), repriced.Hash()) {
			t.Fatalf("hash of %+v is not stable or collides with a shifted or repriced sale", sale)
		}
		changedKey := sale
		changedKey.OperationKey = rapid.String().Draw(t, "operation key")
		if !bytes.Equal(sale.Hash(), changedKey.Hash()) {
			t.Fatal("the operation key is not part of the sale's content and must not change its hash")
		}
	})
}

func TestFaultPlanIsAPureFunctionOfSeedAndKeyWithinTheLatencyRange(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		minimum := time.Duration(rapid.Int64Range(0, int64(time.Second)).Draw(t, "min"))
		faults := Faults{
			Seed:        rapid.Uint64().Draw(t, "seed"),
			LatencyMin:  minimum,
			LatencyMax:  minimum + time.Duration(rapid.Int64Range(0, int64(time.Second)).Draw(t, "spread")),
			TimeoutRate: rapid.Float64Range(0, 1).Draw(t, "timeout rate"),
		}
		key := rapid.String().Draw(t, "operation key")

		plan := faults.Plan(key)
		if plan != faults.Plan(key) {
			t.Fatalf("two plans for %q", key)
		}
		if plan.Latency < faults.LatencyMin || (faults.LatencyMax > faults.LatencyMin && plan.Latency >= faults.LatencyMax) {
			t.Fatalf("latency %v outside [%v, %v)", plan.Latency, faults.LatencyMin, faults.LatencyMax)
		}
		if (faults.TimeoutRate == 0 && plan.Timeout) || (faults.TimeoutRate == 1 && !plan.Timeout) {
			t.Fatalf("timeout %v at rate %v", plan.Timeout, faults.TimeoutRate)
		}
	})
}
