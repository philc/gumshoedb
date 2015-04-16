package hutil

import (
	"net"
	"net/http"
	"strings"
)

var forwardedIPHeaders = []string{"X-Forwarded-For", "Client-IP", "X-Client-IP"} // in priority order

// TODO: we could allow the user to set some variable (TrustedPrivateIPs) to be a list of addresses they wish
// to be treated as private and excluded when determining remote IP. That allows for the proper configuration
// to account for some remote proxy.

func isPrivateIP(ip net.IP) bool {
	// Exclude loopback addresses
	if ip.IsLoopback() {
		return true
	}
	// Private IP ranges: http://en.wikipedia.org/wiki/Private_network
	if ip4 := ip.To4(); ip4 != nil {
		// IPv4
		switch {
		case ip4[0] == 10:
			return true
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] < 32:
			return true
		case ip4[0] == 192 && ip4[1] == 168:
			return true
		}
		return false
	}
	// IPv6: http://en.wikipedia.org/wiki/Unique_Local_Address
	return ip[0] >= 0xfc && ip[0] < 0xfe
}

func RemoteIP(r *http.Request) net.IP {
	// First try the commonly set forwarding address headers. Start from the end and find the first non-private
	// address.
	for _, h := range forwardedIPHeaders {
		header := r.Header.Get(h)
		if header == "" {
			continue
		}
		parts := strings.Split(header, ",")
		for i := len(parts) - 1; i >= 0; i-- {
			ip := net.ParseIP(strings.TrimSpace(parts[i]))
			if !isPrivateIP(ip) {
				return ip
			}
		}
	}

	// Just use RemoteAddr.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	return net.ParseIP(host)
}
