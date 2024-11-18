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
	"example.com/logger"
	"github.com/google/uuid"
)

const (
	server_response_timeout = 10e10           // timeout for the server to respond to a STATUS packet
	interval                = 1 * time.Second // interval between sending packets
	max_key_index           = 100_000         // maximum index for the cache servers
)

var nodeID int
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
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "router", fmt.Sprintf("Resolved %s to %s", addr, serverAddr)))
	}

	/* Dial the server */
	var conn *net.UDPConn
	conn, err = net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return "", err
	} else {
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "router", fmt.Sprintf("Established UDP connection with %s", addr)))
	}

	/* Send a packet to the server */
	_, err = conn.Write([]byte(strconv.FormatInt(payload, 10)))
	if err != nil {
		return "", err
	} else {
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "router", fmt.Sprintf("Sent %d to %s", payload, addr)))
	}

	/* Listen for a response */
	buffer := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(server_response_timeout))
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return "", err
	} else {
		logger.BroadcastLog((logger.GenerateInfoLog(nodeID, "router", fmt.Sprintf("Received %s from %s", string(buffer[:n]), addr))))
	}

	return string(buffer[:n]), nil
}


func main() {
	var err error

	if !populateServers() {
		return // Exit if there are no cache servers
	}

	brokers := []string{"192.168.239.251:9092"}
	topic := "logs"
	err = logger.InitLogger(brokers, topic, false)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	nodeID = int(uuid.New().ID())
	/* TODO Register with the logging system */

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

		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "router", fmt.Sprintf("Response from %s: %s", cacheServers[ipIndex], response)))

		/* Go through the cacheServers in a Round-Robin fashion */
		ipIndex = (ipIndex + 1) % len(cacheServers)
	}
}
