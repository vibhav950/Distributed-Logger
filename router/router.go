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
	server_response_timeout = 2 * time.Second // timeout for the server to respond to a STATUS packet
	interval                = 1 * time.Second // interval between sending packets
	max_key_index           = 100_000         // maximum index for the cache servers
)

var gloablNodeID int
var cacheServers = []string{}

func populateServers() (error, string) {
	var err error
	var file *os.File

	file, err = os.Open("../servers.txt") // Replace with your file name
	if err != nil {
		return err, "Error opening file"
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

	if err = scanner.Err(); err != nil {
		return err, "Error reading file"
	}

	if len(cacheServers) == 0 {
		return fmt.Errorf("No cache servers found"), "No cache servers found"
	}

	return nil, ""
}

func udpSendAndReceive(addr string, payload int64) (string, error) {
	/* Resolve the UDP address */
	serverAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", err
	} else {
		logger.SendInfoLog(gloablNodeID, "router", fmt.Sprintf("Resolved %s to %s", addr, serverAddr))
	}

	/* Dial the server */
	var conn *net.UDPConn
	conn, err = net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return "", err
	} else {
		logger.SendInfoLog(gloablNodeID, "router", fmt.Sprintf("Established UDP connection with %s", addr))
	}

	/* Send a packet to the server */
	_, err = conn.Write([]byte(strconv.FormatInt(payload, 10)))
	if err != nil {
		return "", err
	} else {
		logger.SendInfoLog(gloablNodeID, "router", fmt.Sprintf("Sent %d to %s", payload, addr))
	}

	/* Listen for a response */
	buffer := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(server_response_timeout))
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return "", err
	} else {
		fmt.Printf("Received response from %s: %s\n", addr, string(buffer[:n]))
	}

	return string(buffer[:n]), nil
}

func main() {
	/* Initialize the logger */
	brokers := []string{"localhost:9092"}
	topic := "critical_logs"
	fluentdAddress := "localhost"
	logger.InitLogger(brokers, topic, fluentdAddress)
	defer logger.CloseLogger()

	/* Populate the cache servers */
	err, msg := populateServers()
	if err != nil {
		logger.SendErrorLog(gloablNodeID, "router", msg, "500", fmt.Sprintf("%v", err))
		return
	}

	/* Assign this service a unique ID */
	gloablNodeID = int(uuid.New().ID())

	logger.SendRegistrationMsg(gloablNodeID, "router")

	/* Start the heartbeat routine */
	go logger.StartHeartbeatRoutine(gloablNodeID)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ipIndex := 0
	for range ticker.C {
		payload := (rand.Int63()) % max_key_index
		response, err := udpSendAndReceive(cacheServers[ipIndex], payload)

		if err != nil {
			fmt.Println("Sending error log")
			logger.SendErrorLog(gloablNodeID, "router", "Error sending UDP packet", "500", fmt.Sprintf("%v", err))
			continue
		}

		logger.SendInfoLog(gloablNodeID, "router", fmt.Sprintf("Response from %s: %s", cacheServers[ipIndex], response))

		/* Go through the cacheServers in a Round-Robin fashion */
		ipIndex = (ipIndex + 1) % len(cacheServers)
	}
}
