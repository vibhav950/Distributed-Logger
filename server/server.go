// package main

// import (
//     "fmt"
//     "net"
// )

// func main() {
//     addr := net.UDPAddr{
//         Port: 7777,
//         IP:   net.ParseIP("127.0.0.1"),
//     }
//     conn, err := net.ListenUDP("udp", &addr)
//     if err != nil {
//         fmt.Println("Error starting UDP server:", err)
//         return
//     }
//     defer conn.Close()

//     buffer := make([]byte, 1024)
//     for {
//         n, remoteAddr, err := conn.ReadFromUDP(buffer)
//         if err != nil {
//             fmt.Println("Error reading from UDP:", err)
//             continue
//         }
//         fmt.Printf("Received message from %s: %s\n", remoteAddr, string(buffer[:n]))
//     }
// }

// func main() {
//     addr := net.UDPAddr{
//         Port: 8080,
//         IP:   net.ParseIP("127.0.0.1"),
//     }
//     conn, err := net.ListenUDP("udp", &addr)
//     if err != nil {
//         fmt.Println("Error starting UDP server:", err)
//         return
//     }
//     defer conn.Close()

//     buffer := make([]byte, 1024)
//     for {
//         n, remoteAddr, err := conn.ReadFromUDP(buffer)
//         if err != nil {
//             fmt.Println("Error reading from UDP:", err)
//             continue
//         }
//         fmt.Printf("Received message from %s: %s\n", remoteAddr, string(buffer[:n]))

// 		if bytes.Equal(buffer[:n], []byte("STATUS")) {
// 			_, err = conn.WriteToUDP([]byte("UP"), remoteAddr)
// 			if err != nil {
// 				fmt.Println("Error writing to UDP:", err)
// 			}
// 		}
//     }
// }

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
)

// ElasticClient wraps the Elasticsearch client for indexing logs
type ElasticClient struct {
	Client *elasticsearch.Client
	Index  string
}

// NewElasticClient initializes an Elasticsearch client
func NewElasticClient(index string) (*ElasticClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ElasticClient{Client: client, Index: index}, nil
}

// IndexLog stores a log in Elasticsearch
func (ec *ElasticClient) IndexLog(logData map[string]interface{}) error {
	data, err := json.Marshal(logData)
	if err != nil {
		return err
	}
	res, err := ec.Client.Index(ec.Index, bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return nil
}

func consumeTopic(brokers []string, topic string, ec *ElasticClient, wg *sync.WaitGroup) {
	defer wg.Done()

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("Listening to topic: %s\n", topic)

	for message := range partitionConsumer.Messages() {
		var logData map[string]interface{}
		err := json.Unmarshal(message.Value, &logData)
		if err != nil {
			fmt.Printf("Failed to unmarshal log: %v\n", err)
			continue
		}

		// Store the log in Elasticsearch
		err = ec.IndexLog(logData)
		if err != nil {
			fmt.Printf("Failed to index log: %v\n", err)
		} else {
			fmt.Printf("Log indexed: %s\n", logData)
		}
	}
}

func main() {
	brokers := []string{"172.24.230.157:9092"}
	topics := []string{"registration_logs", "info_logs", "warn_logs", "error_logs", "heartbeat_logs"}
	elasticIndex := "kafka-logs"

	// Initialize Elasticsearch client
	ec, err := NewElasticClient(elasticIndex)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch client: %v", err)
	}

	// Spawn a consumer for each topic
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go consumeTopic(brokers, topic, ec, &wg)
	}

	wg.Wait()
	fmt.Println("All consumers stopped.")
}
