package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"time"
)

const (
	server_response_timeout = 10e10           // timeout for the server to respond to a STATUS packet
	interval                = 5 * time.Second // interval between sending packets
)

var CACHES = []string{
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

/* Get the status of the server, we only consider a server to be
 * online if it responds with "UP" */
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

	if !bytes.Equal(response[:n], []byte("UP")) {
		return false, nil
	}
	return true, nil
}

func main() {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ipIndex := 0
	for range ticker.C {
		// var status bool
		var err error

		/* Check if the server is up */
		// status, err = getServerStatus(CACHES[ipIndex])
		// if (err != nil) || !status {
		// 	fmt.Printf("Server %s is down, skipping...\n", CACHES[ipIndex]);
		// 	ipIndex = (ipIndex + 1) % len(CACHES)
		// 	continue
		// }

		payload := rand.Int63()
		err = sendUDPPacket(CACHES[ipIndex], payload)
		if err != nil {
			fmt.Printf("Error sending packet to %s: %v\n", CACHES[ipIndex], err)
		} else {
			fmt.Printf("Sent packet to %s with payload %d\n", CACHES[ipIndex], payload)
		}

		/* Go through the caches in a Round-Robin fashion */
		ipIndex = (ipIndex + 1) % len(CACHES)
	}
}
