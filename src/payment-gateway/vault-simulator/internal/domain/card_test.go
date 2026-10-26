package domain

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
)

func luhnCheckDigit(body string) string {
	for digit := 0; digit < 10; digit++ {
		if candidate := body + strconv.Itoa(digit); LuhnValid(candidate) {
			return strconv.Itoa(digit)
		}
	}
	return ""
}

func generatedNumber(prefix string, serial int) string {
	body := fmt.Sprintf("%s%011d", prefix, serial)
	return body + luhnCheckDigit(body)
}

type vectorFile struct {
	SyntheticKey string `json:"synthetic_key"`
	TestCards    []struct {
		Number   string `json:"number"`
		CardRef  string `json:"card_ref"`
		Behavior string `json:"behavior"`
		Expected string `json:"expected"`
	} `json:"test_cards"`
}

func TestAllowListMatchesIdentityVectors(t *testing.T) {
	raw, err := os.ReadFile("../../../../../contracts/synthetic/identity-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors vectorFile
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.TestCards) == 0 {
		t.Fatal("no test_cards vectors")
	}
	for _, vector := range vectors.TestCards {
		t.Run(vector.CardRef, func(t *testing.T) {
			if !LuhnValid(vector.Number) {
				t.Fatalf("vector number is not Luhn valid")
			}
			card, ok := LookupCard(vector.Number)
			if !ok {
				t.Fatal("vector card is not allow-listed")
			}
			if card.Ref != vector.CardRef || string(card.Behavior) != vector.Behavior {
				t.Fatalf("got %+v, want ref %s behavior %s", card, vector.CardRef, vector.Behavior)
			}
			if got := Fingerprint([]byte(vectors.SyntheticKey), card.Ref); got != vector.Expected {
				t.Fatalf("fingerprint %s, want %s", got, vector.Expected)
			}
		})
	}
}

func TestLookupCardRejectsOutsideDomain(t *testing.T) {
	for _, r := range generatedRanges {
		card, ok := LookupCard(generatedNumber(r.prefix, CardSerialLimit-1))
		if !ok || card.Ref != "card-99999999999" || card.Behavior != r.behavior {
			t.Fatalf("encoding boundary rejected or mapped incorrectly: %+v", card)
		}
		if _, ok := LookupCard(generatedNumber(r.prefix, CardSerialLimit)); ok {
			t.Fatal("out-of-domain card accepted")
		}
	}
	for _, number := range []string{"4242424242424242", "4111000000000001", "", "4111-0000-0000-0000"} {
		if _, ok := LookupCard(number); ok {
			t.Fatal("invalid or obsolete fixture accepted")
		}
	}
}
