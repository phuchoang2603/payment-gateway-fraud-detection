package domain

import (
	"net/netip"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

var addressPart = rapid.StringMatching(`[A-Za-z0-9 ]{0,24}`)

func address(t *rapid.T) Address {
	return Address{
		Name:       addressPart.Draw(t, "name"),
		Line1:      addressPart.Draw(t, "line1"),
		Line2:      addressPart.Draw(t, "line2"),
		City:       addressPart.Draw(t, "city"),
		Region:     addressPart.Draw(t, "region"),
		PostalCode: addressPart.Draw(t, "postal code"),
		Country:    rapid.SampledFrom([]string{"CA", "US", "VN"}).Draw(t, "country"),
	}
}

// restyle changes case and spacing only, as two entries of the same place typed differently.
func restyle(t *rapid.T, value, label string) string {
	words := strings.Fields(value)
	for i, word := range words {
		if rapid.Bool().Draw(t, label+" upper") {
			words[i] = strings.ToUpper(word)
		} else {
			words[i] = strings.ToLower(word)
		}
	}
	return "  " + strings.Join(words, strings.Repeat(" ", rapid.IntRange(1, 3).Draw(t, label+" spaces"))) + "\t"
}

func TestAddressFingerprintIgnoresCaseSpacingAndRecipient(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		f := NewFingerprinter("key")
		a := address(t)
		b := Address{
			Name:       addressPart.Draw(t, "other name"),
			Line1:      restyle(t, a.Line1, "line1"),
			Line2:      restyle(t, a.Line2, "line2"),
			City:       restyle(t, a.City, "city"),
			Region:     restyle(t, a.Region, "region"),
			PostalCode: restyle(t, a.PostalCode, "postal code"),
			Country:    strings.ToLower(a.Country),
		}

		if f.Address(a) != f.Address(b) {
			t.Fatalf("%q and %q fingerprint differently", NormalizeAddress(a), NormalizeAddress(b))
		}
		if NormalizeAddress(Address{Line1: NormalizeAddress(a)}) != NormalizeAddress(a) {
			t.Fatalf("normalizing %q twice changed it", NormalizeAddress(a))
		}
		if NewFingerprinter("other").Address(a) == f.Address(a) {
			t.Fatal("the key does not change the fingerprint")
		}
	})
}

func TestCardNumberNormalizationAcceptsOnlyGroupedDigitsOfCardLength(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		digits := rapid.StringMatching(`[0-9]{1,24}`).Draw(t, "digits")
		var grouped strings.Builder
		for i, digit := range digits {
			if i > 0 && rapid.IntRange(0, 4).Draw(t, "separator") == 0 {
				grouped.WriteString(rapid.SampledFrom([]string{" ", "-"}).Draw(t, "kind"))
			}
			grouped.WriteRune(digit)
		}

		normalized, ok := NormalizeCardNumber(grouped.String())
		if ok != (len(digits) >= 12 && len(digits) <= 19) || (ok && normalized != digits) {
			t.Fatalf("%q -> %q, %v", grouped.String(), normalized, ok)
		}
		if _, ok := NormalizeCardNumber(grouped.String() + rapid.SampledFrom([]string{"x", ".", "/", "٣"}).Draw(t, "foreign")); ok {
			t.Fatalf("accepted %q with a non-grouping character", grouped.String())
		}
	})
}

func TestNetworkFingerprintIsSharedWithinAPrefixOnly(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		f := NewFingerprinter("key")
		var base [4]byte
		for i := range base {
			base[i] = rapid.Byte().Draw(t, "octet")
		}
		neighbour := base
		neighbour[3] = rapid.Byte().Draw(t, "host")
		other := base
		other[2]++

		ip, near, far := netip.AddrFrom4(base), netip.AddrFrom4(neighbour), netip.AddrFrom4(other)
		a, _ := f.Network(ip)
		b, _ := f.Network(near)
		c, _ := f.Network(far)
		mapped, _ := f.Network(netip.AddrFrom16(ip.As16()))
		if a != b || a == c || a != mapped {
			t.Fatalf("%v, %v, %v and its IPv6-mapped form fingerprint as %s %s %s %s", ip, near, far, a, b, c, mapped)
		}
	})
}
