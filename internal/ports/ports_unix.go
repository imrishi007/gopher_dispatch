//go:build !windows

package ports

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func find(port int) ([]Owner, error) {
	b, err := exec.Command("ss", "-ltnp").Output()
	if err != nil {
		return nil, err
	}
	needle := fmt.Sprintf(":%d", port)
	var result []Owner
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.Contains(line, needle) {
			continue
		}
		for _, field := range strings.Fields(line) {
			if i := strings.Index(field, "pid="); i >= 0 {
				pid, _ := strconv.Atoi(strings.Trim(field[i+4:], ",)\""))
				if pid > 0 {
					result = append(result, Owner{Port: port, PID: pid, Protocol: "tcp"})
				}
			}
		}
	}
	return result, nil
}
