package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
)

type Behavior string

const (
	BehaviorOK      Behavior = "ok"
	BehaviorNSF     Behavior = "nsf"
	BehaviorIssuer  Behavior = "issuer"
	BehaviorInvalid Behavior = "invalid"
)

const (
	MethodTypeCard   = "card"
	CardSerialLimit  = 100_000_000_000
	cardNumberLength = 16
)

type Card struct {
	Ref      string
	Behavior Behavior
}

var generatedRanges = []struct {
	prefix   string
	behavior Behavior
}{
	{"4111", BehaviorOK},
	{"4112", BehaviorNSF},
	{"4113", BehaviorIssuer},
	{"4114", BehaviorInvalid},
}

func LookupCard(number string) (Card, bool) {
	if len(number) != cardNumberLength || !allDigits(number) || !LuhnValid(number) {
		return Card{}, false
	}
	for _, r := range generatedRanges {
		if number[:4] != r.prefix {
			continue
		}
		serial, err := strconv.Atoi(number[4 : cardNumberLength-1])
		if err != nil || serial >= CardSerialLimit {
			return Card{}, false
		}
		return Card{
			Ref:      fmt.Sprintf("card-%06d", serial),
			Behavior: r.behavior,
		}, true
	}
	return Card{}, false
}

func LuhnValid(number string) bool {
	if number == "" || !allDigits(number) {
		return false
	}
	sum := 0
	double := false
	for i := len(number) - 1; i >= 0; i-- {
		digit := int(number[i] - '0')
		if double {
			digit *= 2
			if digit > 9 {
				digit -= 9
			}
		}
		sum += digit
		double = !double
	}
	return sum%10 == 0
}

func Fingerprint(key []byte, cardRef string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(cardRef))
	return hex.EncodeToString(mac.Sum(nil))
}

func allDigits(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
