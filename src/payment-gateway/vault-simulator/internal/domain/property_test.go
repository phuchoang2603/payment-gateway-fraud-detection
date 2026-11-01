package domain

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// withCheckDigit appends the digit that makes the number pass the Luhn check.
func withCheckDigit(body string) string {
	for digit := range 10 {
		candidate := fmt.Sprintf("%s%d", body, digit)
		if LuhnValid(candidate) {
			return candidate
		}
	}
	panic("no Luhn check digit for " + body)
}

func TestGeneratedCardsResolveToTheirSerialAndBehavior(t *testing.T) {
	behaviors := map[string]Behavior{"4111": BehaviorOK, "4112": BehaviorNSF, "4113": BehaviorIssuer, "4114": BehaviorInvalid}
	rapid.Check(t, func(t *rapid.T) {
		prefix := rapid.SampledFrom([]string{"4111", "4112", "4113", "4114"}).Draw(t, "prefix")
		serial := rapid.Int64Range(0, CardSerialLimit-1).Draw(t, "serial")
		number := withCheckDigit(fmt.Sprintf("%s%011d", prefix, serial))

		card, ok := LookupCard(number)
		if !ok || card.Ref != fmt.Sprintf("card-%06d", serial) || card.Behavior != behaviors[prefix] {
			t.Fatalf("%s -> %+v, %v", number, card, ok)
		}
	})
}

func TestChangingOneDigitBreaksTheLuhnCheck(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		number := withCheckDigit(rapid.StringMatching(`[0-9]{11,18}`).Draw(t, "body"))
		position := rapid.IntRange(0, len(number)-1).Draw(t, "position")
		shift := byte(rapid.IntRange(1, 9).Draw(t, "shift"))

		changed := []byte(number)
		changed[position] = '0' + (changed[position]-'0'+shift)%10
		if !LuhnValid(number) || LuhnValid(string(changed)) {
			t.Fatalf("%s valid=%v, %s valid=%v", number, LuhnValid(number), changed, LuhnValid(string(changed)))
		}
		if _, ok := LookupCard(string(changed)); ok {
			t.Fatalf("looked up %s, which fails the Luhn check", changed)
		}
	})
}
