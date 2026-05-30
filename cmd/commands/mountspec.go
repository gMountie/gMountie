package commands

import (
	"fmt"
	"strconv"
	"strings"
)

const defaultServerPort = 9449

// mountSpec is the parsed form of [user@]host[:port]/volume.
type mountSpec struct {
	Username string
	Host     string
	Port     int
	Volume   string
}

// parseMountSpec parses the sshfs-style shorthand "[user@]host[:port]/volume".
// Port defaults to 9449. Host and volume are required.
func parseMountSpec(s string) (mountSpec, error) {
	const example = `expected [user@]host[:port]/volume, e.g. admin@host:9449/shared`
	var spec mountSpec

	if at := strings.IndexByte(s, '@'); at >= 0 {
		spec.Username = s[:at]
		s = s[at+1:]
		if spec.Username == "" {
			return spec, fmt.Errorf("empty username before '@'; %s", example)
		}
	}

	slash := strings.IndexByte(s, '/')
	if slash < 0 {
		return spec, fmt.Errorf("missing '/volume'; %s", example)
	}
	hostport := s[:slash]
	spec.Volume = s[slash+1:]
	if spec.Volume == "" {
		return spec, fmt.Errorf("empty volume after '/'; %s", example)
	}

	spec.Port = defaultServerPort
	if colon := strings.IndexByte(hostport, ':'); colon >= 0 {
		spec.Host = hostport[:colon]
		portStr := hostport[colon+1:]
		p, err := strconv.Atoi(portStr)
		if err != nil || p < 1 || p > 65535 {
			return spec, fmt.Errorf("invalid port %q; %s", portStr, example)
		}
		spec.Port = p
	} else {
		spec.Host = hostport
	}
	if spec.Host == "" {
		return spec, fmt.Errorf("missing host; %s", example)
	}
	return spec, nil
}
