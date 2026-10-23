//go:build windows

package ports

import (
	"sort"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Adapted from github.com/shravanasati/rekt under the MIT License.
const (
	afInet                  = 2
	afInet6                 = 23
	tcpTableOwnerPIDAll     = 5
	errorInsufficientBuffer = 122
)

var (
	iphlpapi            = syscall.NewLazyDLL("iphlpapi.dll")
	getExtendedTCPTable = iphlpapi.NewProc("GetExtendedTcpTable")
)

type tcp4Row struct{ state, localAddr, localPort, remoteAddr, remotePort, pid uint32 }
type tcp6Row struct {
	localAddr                             [16]byte
	localScopeID, localPort               uint32
	remoteAddr                            [16]byte
	remoteScopeID, remotePort, state, pid uint32
}

func find(port int) ([]Owner, error) {
	owners := map[int]string{}
	if err := collect4(port, owners); err != nil {
		return nil, err
	}
	if err := collect6(port, owners); err != nil {
		return nil, err
	}
	result := make([]Owner, 0, len(owners))
	for pid, protocol := range owners {
		result = append(result, Owner{Port: port, PID: pid, Process: processName(pid), Protocol: protocol})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PID < result[j].PID })
	return result, nil
}

func collect4(port int, owners map[int]string) error {
	table, err := tableFor(afInet)
	if err != nil || len(table) < 4 {
		return err
	}
	count := *(*uint32)(unsafe.Pointer(&table[0]))
	base := uintptr(unsafe.Pointer(&table[0])) + unsafe.Sizeof(count)
	size := unsafe.Sizeof(tcp4Row{})
	for i := uint32(0); i < count; i++ {
		row := (*tcp4Row)(unsafe.Pointer(base + uintptr(i)*size))
		if hostPort(row.localPort) == port {
			owners[int(row.pid)] = "tcp"
		}
	}
	return nil
}
func collect6(port int, owners map[int]string) error {
	table, err := tableFor(afInet6)
	if err != nil || len(table) < 4 {
		return err
	}
	count := *(*uint32)(unsafe.Pointer(&table[0]))
	base := uintptr(unsafe.Pointer(&table[0])) + unsafe.Sizeof(count)
	size := unsafe.Sizeof(tcp6Row{})
	for i := uint32(0); i < count; i++ {
		row := (*tcp6Row)(unsafe.Pointer(base + uintptr(i)*size))
		if hostPort(row.localPort) == port {
			owners[int(row.pid)] = "tcp6"
		}
	}
	return nil
}
func tableFor(af uint32) ([]byte, error) {
	var size uint32
	code, _, _ := getExtendedTCPTable.Call(0, uintptr(unsafe.Pointer(&size)), 0, uintptr(af), tcpTableOwnerPIDAll, 0)
	if uint32(code) != errorInsufficientBuffer && code != 0 {
		return nil, syscall.Errno(code)
	}
	data := make([]byte, size)
	code, _, _ = getExtendedTCPTable.Call(uintptr(unsafe.Pointer(&data[0])), uintptr(unsafe.Pointer(&size)), 0, uintptr(af), tcpTableOwnerPIDAll, 0)
	if code != 0 {
		return nil, syscall.Errno(code)
	}
	return data, nil
}
func hostPort(v uint32) int { p := uint16(v); return int(p>>8 | p<<8) }
func processName(pid int) string {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return ""
	}
	defer windows.CloseHandle(handle)
	buf := make([]uint16, windows.MAX_PATH)
	n := uint32(len(buf))
	if windows.QueryFullProcessImageName(handle, 0, &buf[0], &n) != nil {
		return ""
	}
	return windows.UTF16ToString(buf[:n])
}
