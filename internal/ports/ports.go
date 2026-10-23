package ports

import (
	"fmt"
)

type Owner struct {
	Port     int    `json:"port"`
	PID      int    `json:"pid"`
	Process  string `json:"process,omitempty"`
	Protocol string `json:"protocol"`
}

func Find(port int) ([]Owner, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("port must be between 1 and 65535")
	}
	return find(port)
}
