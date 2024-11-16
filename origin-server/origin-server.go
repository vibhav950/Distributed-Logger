package main

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

var dictionary = make(map[int]string)

func generateRandomString() string {
	// Generate a random string of length 10
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, 10)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	log.Println("Starting the origin server")
	log.Println("Generating random strings")
	for i := 1; i <= 10000; i++ {
		dictionary[i] = generateRandomString()
	}
	log.Println("Random strings generated")

	pc, err := net.ListenPacket("udp", ":7777")
	log.Println("Listening on port 7777")
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()

	for {
		buffer := make([]byte, 1024)
		n, addr, err := pc.ReadFrom(buffer)
		if err != nil {
			log.Println("Error reading from UDP:", err)
			continue
		}
		log.Printf("Received message from %s: %s\n", addr, string(buffer[:n]))

		key, err := strconv.Atoi(string(buffer[:n-1]))
		if err != nil {
			log.Println("Error converting key to int:", err)
			continue
		}
		value, ok := dictionary[key]
		if !ok {
			log.Println("Key not found")
			continue
		}
		log.Println("Sending value to client", value)
		_, err = pc.WriteTo([]byte(value), addr)
		if err != nil {
			log.Println("Error writing to UDP:", err)
		}

	}

}
