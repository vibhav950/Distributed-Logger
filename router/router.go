package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	server_response_timeout = 10e10           // timeout for the server to respond to a STATUS packet
	interval                = 1 * time.Second // interval between sending packets
	max_key_index		 	= 100_000         // maximum index for the cache servers
)

var cacheServers = []string{}

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

	return true
}

func udpSendAndReceive(addr string, payload int64) (string, error) {
	/* Resolve the UDP address */
	serverAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", err
	} else {
		fmt.Printf("Resolved %s to %s\n", addr, serverAddr)
	}

	/* Dial the server */
	var conn *net.UDPConn
	conn, err = net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return "", err
	} else {
		fmt.Printf("Established UDP connection with %s\n", addr)
	}

	/* Send a packet to the server */
	_, err = conn.Write([]byte(strconv.FormatInt(payload, 10)))
	if err != nil {
		return "", err
	} else {
		fmt.Printf("Sent %d to %s\n", payload, addr)
	}

	/* Listen for a response */
	buffer := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(server_response_timeout))
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return "", err
	} else {
		fmt.Printf("Received from %s\n", addr)
	}

	return string(buffer[:n]), nil
}

func main() {
	if !populateServers() {
		return // Exit if there are no cache servers
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ipIndex := 0
	for range ticker.C {

		payload := (rand.Int63()) % max_key_index
		response, err := udpSendAndReceive(cacheServers[ipIndex], payload)

		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Printf("Response from %s: %s\n", cacheServers[ipIndex], response)

		/* Go through the cacheServers in a Round-Robin fashion */
		ipIndex = (ipIndex + 1) % len(cacheServers)
	}
}
