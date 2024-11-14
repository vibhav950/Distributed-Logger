package main

import (
    "fmt"
    "math/rand"
    "net"
    "time"
	"bytes"
)

const (
	server_response_timeout = 10e10 // timeout for the server to respond to a STATUS packet
    interval = 5 * time.Second // interval between sending packets
)

var IPS = []string{
    "127.0.0.1:8080",
}

func sendUDPPacket(ip string, payload int64) error {
    conn, err := net.Dial("udp", ip)
    if err != nil {
        return err
    }
    defer conn.Close()

    _, err = fmt.Fprintf(conn, "%d", payload)
    return err
}

// Get the status of the server by sending the STATUS string to the server
// If the server is up, it will respond with UP, else DOWN
// If the server does not respond in server_response_timeout seconds, it is considered to be offline
func getServerStatus(ip string) (bool, error) {
	var n int

	conn, err := net.DialTimeout("udp", ip, server_response_timeout)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	_, err = fmt.Fprintf(conn, "STATUS")
	if err != nil {
		return false, err
	}

	response := make([]byte, 5)
	n, err = conn.Read(response)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(response [:n], []byte("UP")) {
		return false, nil
	}
	return true, nil
}

func main() {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    ipIndex := 0
    for range ticker.C {
		var status bool
		var err error

		// Check if the server is up
		// If the server is down, skip sending the packet
		status, err = getServerStatus(IPS[ipIndex])
		if (err != nil) || !status {
			fmt.Printf("Server %s is down, skipping...\n", IPS[ipIndex]);
			ipIndex = (ipIndex + 1) % len(IPS)
			continue
		}

        payload := rand.Int63()
        err = sendUDPPacket(IPS[ipIndex], payload)
        if err != nil {
            fmt.Printf("Error sending packet to %s: %v\n", IPS[ipIndex], err)
        } else {
            fmt.Printf("Sent packet to %s with payload %d\n", IPS[ipIndex], payload)
        }

		// Go through the IPs in a Round-Robin fashion
        ipIndex = (ipIndex + 1) % len(IPS)
    }
}