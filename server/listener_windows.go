//go:build windows

package server

import "net"

func listenUnixPrivate(path string) (net.Listener, error) { return net.Listen("unix", path) }
