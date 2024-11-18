package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.com/logger"
	"github.com/google/uuid"
)

var cache sync.Map
var cacheSize int
var cacheMutex sync.Mutex

var a logger.RegistrationLog
var brokers = []string{"192.168.239.251:9092"}
var nodeID int

const topic = "cache_logs"
const maxCacheSize = 1000

var originServers []string
var count int
var mu sync.Mutex

const nkeys = 100_000

func main() {
	nodeID = int(uuid.New().ID())
	err := logger.InitLogger(brokers, topic, false)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		return
	}
	log.Println("Logger initialized")

	log.Printf("Starting cache server with unique ID: %d\n", nodeID)
	logger.BroadcastLog(logger.GenerateRegistrationLog(nodeID, "cache_server"))

	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a host:port string in the input.")
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "No host:port provided in arguments", "ARGUMENT_ERROR", "Missing input argument"))
		return
	}
	CONNECT := arguments[1]

	addr, err := net.ResolveUDPAddr("udp", CONNECT)
	if err != nil {
		fmt.Printf("Error resolving address: %v\n", err)
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to resolve UDP address", "ADDRESS_ERROR", fmt.Sprintf("%v", err)))
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err)
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to start UDP listener", "LISTEN_ERROR", fmt.Sprintf("%v", err)))
		return
	}
	defer conn.Close()

	fmt.Printf("UDP server is listening on %s\n", CONNECT)
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Cache server is listening on %s", CONNECT)))

	if !populateServers() {
		fmt.Println("Error populating servers.")
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to populate origin servers", "CONFIG_ERROR", "Check servers.txt configuration"))
		return
	}

	bufferPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024)
		},
	}

	// handle incoming packets with go routines
	for {
		// ReadFrom reads a packet from the connection,
		// copying the payload into p. It returns the number of
		// bytes copied into p and the return address that
		// was on the packet.
		// It returns the number of bytes read (0 <= n <= len(p))
		// and any error encountered. Callers should always process
		// the n > 0 bytes returned before considering the error err.
		// ReadFrom can be made to time out and return an error after a
		// fixed time limit; see SetDeadline and SetReadDeadline.

		buffer := bufferPool.Get().([]byte)
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("Error while reading from UDP: %v\n", err)
			logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Error reading UDP packet", "NETWORK_ERROR", fmt.Sprintf("%v", err)))
			bufferPool.Put(buffer)
			continue
		}

		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Received %d bytes from %s", n, clientAddr.String())))
		go func(data []byte, length int, addr *net.UDPAddr) {
			defer bufferPool.Put(buffer)
			handlePacket(conn, data[:length], addr)
		}(buffer, n, clientAddr)
	}
}

func handlePacket(conn *net.UDPConn, data []byte, addr *net.UDPAddr) {
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Processing packet from %s", addr.String())))

	key, err := strconv.Atoi(string(data))
	if err != nil {
		fmt.Printf("Error while converting from string to int: %v\n", err)
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to convert data to integer", "DATA_ERROR", fmt.Sprintf("%v", err)))
		return
	}

	response := getVal(key)
	if response == "" {
		fmt.Printf("Error retrieving data.\n")
		logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "cache_server", fmt.Sprintf("Data for key %d not found in cache or origin servers", key)))
		return
	}

	_, err = conn.WriteToUDP([]byte(response), addr)
	if err != nil {
		fmt.Printf("Error writing to UDP: %v\n", err)
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to send UDP response", "WRITE_ERROR", fmt.Sprintf("%v", err)))
	}
}

func getVal(key int) string {
	value, ok := cache.Load(key)
	if !ok {
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Cache miss for key: %d", key)))
		val, err := getFromOrigin(key)
		if err != nil || val == "" {
			logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "cache_server", fmt.Sprintf("Key %d not found in origin servers", key)))
			return ""
		}
		addToCache(key, val)
		return val
	}
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Cache hit for key: %d", key)))
	return value.(string)
}

func addToCache(key int, val string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if cacheSize >= maxCacheSize {
		logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "cache_server", "Cache size exceeded limit, removing random key"))
		removeRandomKey()
	}

	cache.Store(key, val)
	cacheSize++
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Added key %d to cache", key)))
}

// v cool
func removeRandomKey() {
	cache.Range(func(k, v interface{}) bool {
		// delete the first key we encounter (random enough for this use case)
		cache.Delete(k)
		cacheSize--
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Removed random key: %v from cache", k)))
		return false
	})
}

func getFromOrigin(key int) (string, error) {
	mu.Lock()
	addr := originServers[count]
	count = (count + 1) % len(originServers)
	mu.Unlock()

	serverAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to resolve origin server address", "ADDRESS_ERROR", fmt.Sprintf("%v", err)))
		return "", err
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to connect to origin server", "CONNECTION_ERROR", fmt.Sprintf("%v", err)))
		return "", err
	}
	defer conn.Close()

	for attempt := 0; attempt < 6; attempt++ {
		message := []byte(fmt.Sprintf("%d", key))
		_, err = conn.Write(message)
		if err != nil {
			logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Error writing to origin server", "WRITE_ERROR", fmt.Sprintf("%v", err)))
			return "", err
		}

		conn.SetReadDeadline(time.Now().Add(6 * time.Second))

		buffer := make([]byte, 256)
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "cache_server", fmt.Sprintf("Timeout while waiting for response from origin server (attempt %d)", attempt+1)))
				continue
			}
			logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Error reading from origin server", "READ_ERROR", fmt.Sprintf("%v", err)))
			return "", err
		}

		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", fmt.Sprintf("Received response from origin server for key: %d", key)))
		return string(buffer[:n]), nil
	}

	logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", fmt.Sprintf("Failed to retrieve key %d from origin servers after retries", key), "RETRY_ERROR", "Exceeded max retries"))
	return "", err
}

func populateServers() bool {
	file, err := os.Open("../servers.txt")
	if err != nil {
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Failed to open servers.txt", "FILE_ERROR", fmt.Sprintf("%v", err)))
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inOriginSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "origin_servers" {
			inOriginSection = true
			continue
		}

		if inOriginSection {
			if line == "" || strings.Contains(line, "_servers") {
				break
			}
			originServers = append(originServers, line)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "Error reading servers.txt", "FILE_READ_ERROR", fmt.Sprintf("%v", err)))
		return false
	}

	if len(originServers) == 0 {
		logger.BroadcastLog(logger.GenerateErrorLog(nodeID, "cache_server", "No origin servers found in servers.txt", "CONFIG_ERROR", "Check servers.txt configuration"))
		return false
	}

	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "cache_server", "Origin servers populated successfully"))
	return true
}
