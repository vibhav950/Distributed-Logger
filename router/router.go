package main

import (
	"fmt"
	"math/rand"
	"net"
	"time"
	"os"
	"strings"
	"bufio"
)

const (
	server_response_timeout = 10e10           // timeout for the server to respond to a STATUS packet
	interval                = 5 * time.Second // interval between sending packets
	max_key_index		 	= 100_000         // maximum index for the cache servers
)

var cacheServers = []string{}
var count int

func populateServers() bool {
	file, err := os.Open("../servers.txt") // Replace with your file name
	if err != nil {
		fmt.Println("Error opening file:", err)
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inOriginSection := false

	// Read the file line by line
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "cache_servers" {
			inOriginSection = true
			continue
		}

		// If in the "cache_servers" section and encounter a blank line or another section
		if inOriginSection {
			if line == "" || strings.Contains(line, "_servers") {
				break
			}
			cacheServers = append(cacheServers, line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return false
	}

	if len(cacheServers) == 0 {
		fmt.Println("No origin servers found in the configuration.")
		return false
	}

	count = 0
	return true
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

func main() {
	if !populateServers() {
		return // Exit if there are no cache servers
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ipIndex := 0
	for range ticker.C {
		// var status bool
		var err error

		payload := (rand.Int63()) % max_key_index
		err = sendUDPPacket(cacheServers[ipIndex], payload)
		if err != nil {
			fmt.Printf("Error sending packet to %s: %v\n", cacheServers[ipIndex], err)
		} else {
			fmt.Printf("Sent packet to %s with payload %d\n", cacheServers[ipIndex], payload)
		}

		/* Go through the cacheServers in a Round-Robin fashion */
		ipIndex = (ipIndex + 1) % len(cacheServers)
	}
}
