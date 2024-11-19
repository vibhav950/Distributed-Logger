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
	"time"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/fatih/color"
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
		Username:  "elastic",              // Add your username here
		Password:  "XXiqV27E1FcB*Qeh0jox", // Add your password here

	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ElasticClient{Client: client, Index: index}, nil
}

// IndexLog stores a log in Elasticsearch
func (ec *ElasticClient) IndexLog(logData map[string]interface{}) error {
	infoColor := color.New(color.FgGreen).SprintFunc()
	warnColor := color.New(color.FgYellow).SprintFunc()
	errorColor := color.New(color.FgRed).SprintFunc()
	otherColor := color.New(color.FgBlue).SprintFunc()
	messageColor := color.New(color.FgWhite).SprintFunc()
	timeColor := color.New(color.FgHiWhite).SprintFunc()
	serviceColor := color.New(color.FgCyan).SprintFunc()

	// Print the log message with color based on the log level
	switch logData["log_level"] {
	case "INFO":
		fmt.Printf("  %s - %s [%s] - %s\n", infoColor(logData["log_level"]), messageColor(logData["message"]), serviceColor(logData["service_name"]), timeColor(time.Now().Format("2006-01-02 15:04:05")))
	case "WARN":
		fmt.Printf("  %s - %s [%s] - %s\n", warnColor(logData["log_level"]), messageColor(logData["message"]), serviceColor(logData["service_name"]), timeColor(time.Now().Format("2006-01-02 15:04:05")))
	case "ERROR":
		fmt.Printf("  %s - %s [%s] - %s\n", errorColor(logData["log_level"]), messageColor(logData["message"]), serviceColor(logData["service_name"]), timeColor(time.Now().Format("2006-01-02 15:04:05")))
	default:
		fmt.Printf("  %s - %s [%s] - %s\n", otherColor(logData["message_type"]), messageColor(logData["status"]), serviceColor(logData["service_name"]), timeColor(time.Now().Format("2006-01-02 15:04:05")))
	}
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

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
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

		if len(logData) == 4 {
			// registration message
			logData["status"] = "UP"
		}

		// Store the log in Elasticsearch
		err = ec.IndexLog(logData)
		if err != nil {
			fmt.Printf("Failed to index log: %v\n", err)
		}
		// Search for all documents in the index
		// searchRes, err := ec.Client.Search(
		// 	ec.Client.Search.WithIndex(ec.Index),
		// 	ec.Client.Search.WithPretty(),
		// )
		// if err != nil {
		// 	fmt.Printf("failed to retrieve all documents: %s", err)
		// }

		// // Print the entire database
		// var searchResult map[string]interface{}
		// if err := json.NewDecoder(searchRes.Body).Decode(&searchResult); err != nil {
		// 	fmt.Printf("failed to decode search result: %s", err)
		// }
		// // Pretty-print the search results
		// pretty, _ := json.MarshalIndent(searchResult, "", "  ")
		// fmt.Printf("Entire Database:\n%s\n", string(pretty))

		// searchRes.Body.Close()
	}

}

func main() {
	brokers := []string{"localhost:9092"}
	topics := []string{"logs", "critical_logs"}
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
