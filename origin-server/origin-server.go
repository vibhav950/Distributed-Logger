package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"example.com/logger"
	"github.com/google/uuid"
)

var dictionary = make(map[int]string)
var a logger.RegistrationLog

var brokers = []string{"192.168.239.251:9092"}

const topic = "logs"

var nodeID int

const max_key_size = 100_000

func generateRandomString() string {
	// Generate a random string of length 10
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano())) // initialize local pseudorandom generator
	b := make([]byte, 10)

	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))] // pick a random character from charset
	}
	return string(b)
}

func main() {

	nodeID = int(uuid.New().ID()) // Generate a random node ID
	err := logger.InitLogger(brokers, topic, false)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	log.Println("Starting the origin server with unique ID:", nodeID)
	logger.BroadcastLog(logger.GenerateRegistrationLog(nodeID, "origin-server")) // Broadcast the registration log

	log.Println("Generating random strings")
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "origin-server", "Generating random strings"))

	for i := 1; i <= max_key_size; i++ {
		dictionary[i] = generateRandomString() // Generate a random string for each key
	}
	log.Println("Random strings generated")
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "origin-server ", "Random strings generated"))

	pc, err := net.ListenPacket("udp", ":7777") // Listen on port 7777
	log.Println("Listening on port 7777")
	logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "origin-server", "Listening on port 7777"))

	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()
	// go client()
	for {
		buffer := make([]byte, 1024)        // Create a buffer to read the message
		n, addr, err := pc.ReadFrom(buffer) // Read the message
		if err != nil {
			log.Println("Error reading from UDP:", err)
			logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "origin-server", "Error reading from UDP"))
			continue
		}
		log.Printf("Received message from %s: %s\n", addr, string(buffer[:n]))
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "origin-server", "Received message from "+addr.String()+": "+string(buffer[:n])))

		key, err := strconv.Atoi(string(buffer[:n-1])) // Convert the message to an integer (remove the newline character)
		if err != nil {
			log.Println("Error converting key to int:", err)
			logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "origin-server", "Error converting key to int"))
			continue
		}
		value, ok := dictionary[key] // Get the value from the dictionary
		if !ok {
			log.Println("Key not found")
			logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "origin-server", "Key not found"))
			continue
		}
		log.Println("Sending value to client", value)
		logger.BroadcastLog(logger.GenerateInfoLog(nodeID, "origin-server", "Sending value to client "+value))
		_, err = pc.WriteTo([]byte(value), addr) // Send the value to the client
		if err != nil {
			log.Println("Error writing to UDP:", err)
			logger.BroadcastLog(logger.GenerateWarnLog(nodeID, "origin-server", "Error writing to UDP"))
		}

	}

}
