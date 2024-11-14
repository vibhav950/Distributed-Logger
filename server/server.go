package main

import (
    "fmt"
    "net"
	"bytes"
)

func main() {
    addr := net.UDPAddr{
        Port: 8080,
        IP:   net.ParseIP("127.0.0.1"),
    }
    conn, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Println("Error starting UDP server:", err)
        return
    }
    defer conn.Close()

    buffer := make([]byte, 1024)
    for {
        n, remoteAddr, err := conn.ReadFromUDP(buffer)
        if err != nil {
            fmt.Println("Error reading from UDP:", err)
            continue
        }
        fmt.Printf("Received message from %s: %s\n", remoteAddr, string(buffer[:n]))

		if bytes.Equal(buffer[:n], []byte("STATUS")) {
			_, err = conn.WriteToUDP([]byte("UP"), remoteAddr)
			if err != nil {
				fmt.Println("Error writing to UDP:", err)
			}
		}
    }
}