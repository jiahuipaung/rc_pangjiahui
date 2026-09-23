package destination

import (
	"fmt"
	"net"
)

func ValidateResolvedIPs(addresses []net.IP) error {
	if len(addresses) == 0 {
		return fmt.Errorf("destination resolved to no addresses")
	}
	for _, address := range addresses {
		if disallowedIP(address) {
			return fmt.Errorf("disallowed destination network")
		}
	}
	return nil
}
