package httptransport

import (
	"net/http"
	"net/netip"
	"strings"
)

func (s *Server) clientAddress(r *http.Request) (netip.Addr, bool) {
	var entries []string
	for _, line := range r.Header.Values(s.opts.TrustedProxyHeader) {
		for _, part := range strings.Split(line, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				entries = append(entries, trimmed)
			}
		}
	}
	index := len(entries) - s.opts.TrustedProxyHops
	if index < 0 {
		return netip.Addr{}, false
	}
	entry := entries[index]
	if address, err := netip.ParseAddr(entry); err == nil {
		return address, true
	}
	if addrPort, err := netip.ParseAddrPort(entry); err == nil {
		return addrPort.Addr(), true
	}
	return netip.Addr{}, false
}
