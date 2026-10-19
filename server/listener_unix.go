//go:build !windows

package server

import (
	"net"
	"syscall"
)

func listenUnixPrivate(path string) (net.Listener, error) {
	old := syscall.Umask(0o177)
	defer syscall.Umask(old)
	return net.Listen("unix", path)
}
