// what all does this need to do?

// listen for requests from requesters
// check if the required data is available
// if yes, return with required data
// if no, request data from origin server, return that data to the requester, after saving locally too

// cache size -> let's go with a random number like a 1000 keys? we'll start off with 0 data cached

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// var cache = make(map[int]string)
var cache sync.Map
var cacheSize int
var cacheMutex sync.Mutex

const maxCacheSize = 1000

var originServers []string
var count int
var mu sync.Mutex

const nkeys = 100_000

// need to be passing the ips of the origin servers
func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a host:port string in the input.")
		return
	}
	CONNECT := arguments[1]

	addr, err := net.ResolveUDPAddr("udp", CONNECT)
	if err != nil {
		fmt.Printf("Error resolving address: %v\n", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error listening: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Printf("UDP server is listening on %s\n", CONNECT)

	if !populateServers() {
		fmt.Printf("Error Populating servers\n")
		return
	}

	bufferPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024)
		},
	}

	// Handle incoming packets
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
			bufferPool.Put(buffer)
			continue
		}

		go func(data []byte, length int, addr *net.UDPAddr) {
			defer bufferPool.Put(buffer)
			handlePacket(conn, data[:length], addr)
		}(buffer, n, clientAddr)
	}
}

func handlePacket(conn *net.UDPConn, data []byte, addr *net.UDPAddr) {
	fmt.Printf("Received %d bytes from %s: %s\n", len(data), addr.String(), string(data))
	key, err := strconv.Atoi(string(data))
	if err != nil {
		fmt.Printf("Error while converting from string to int: %v\n", err)
		return
	}

	response := getVal(key)
	if response == "" {
		fmt.Printf("Error retrieving data.\n")
		return
	}

	_, err = conn.WriteToUDP([]byte(response), addr)
	if err != nil {
		fmt.Printf("Error writing to UDP: %v\n", err)
	}
}

func getVal(key int) string {
	value, ok := cache.Load(key)
	if !ok {
		val, err := getFromOrigin(key)
		if err != nil || val == "" {
			return ""
		}
		addToCache(key, val)
		return val
	}
	return value.(string)
}

func addToCache(key int, val string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if cacheSize >= maxCacheSize {
		removeRandomKey()
	}

	cache.Store(key, val)
	cacheSize++
}

// v cool
func removeRandomKey() {
	cache.Range(func(k, v interface{}) bool {
		// delete the first key we encounter (random enough for this use case)
		cache.Delete(k)
		cacheSize--
		// stop the iteration after deleting one key
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
		fmt.Printf("Error resolving origin server: %v\n", err)
		return "", err
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Printf("Error connecting to origin server: %v\n", err)
		return "", err
	}
	defer conn.Close()

	for attempt := 0; attempt < 6; attempt++ {
		message := []byte(fmt.Sprintf("%d", key))
		_, err = conn.Write(message)
		if err != nil {
			fmt.Printf("Error writing to server: %v\n", err)
			return "", err
		}

		conn.SetReadDeadline(time.Now().Add(6 * time.Second))

		buffer := make([]byte, 256)
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			// check if timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Printf("Attempt %d: Timeout\n", attempt+1)
				continue // Retry
			}
			fmt.Printf("Error reading from server: %v\n", err)
			return "", err
		}

		return string(buffer[:n]), nil
	}

	return "", err
}

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
		if line == "origin_servers" {
			inOriginSection = true
			continue
		}

		// If in the "origin_servers" section and encounter a blank line or another section
		if inOriginSection {
			if line == "" || strings.Contains(line, "_servers") {
				break
			}
			originServers = append(originServers, line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return false
	}

	if len(originServers) == 0 {
		fmt.Println("No origin servers found in the configuration.")
		return false
	}

	count = 0
	return true
}
