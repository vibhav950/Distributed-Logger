// package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"

// 	"github.com/IBM/sarama"
// )

// type LogEntry struct {
// 	LogID       float64 `json:"log_id"`
// 	NodeID      float64 `json:"node_id"`
// 	LogLevel    string  `json:"log_level"`
// 	MessageType string  `json:"message_type"`
// 	Message     string  `json:"message"`
// 	ServiceName string  `json:"service_name"`
// 	Timestamp   string  `json:"timestamp"`
// }

// func consumeTopic(consumer sarama.Consumer, topic string) {
// 	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
// 	if err != nil {
// 		log.Fatalf("Error creating partition consumer for topic %s: %v", topic, err)
// 	}
// 	defer partitionConsumer.Close()

// 	fmt.Printf("Listening for logs on topic: %s...\n", topic)
// 	var logEntry LogEntry

// 	for message := range partitionConsumer.Messages() {
// 		err := json.Unmarshal(message.Value, &logEntry)
// 		if err != nil {
// 			log.Printf("Error unmarshalling message from topic %s: %v", topic, err)
// 			continue
// 		}
// 		fmt.Printf("Message received on topic %s: %s, Service: %s\n", topic, string(message.Value), logEntry.ServiceName)
// 	}
// }

// func main() {
// 	brokers := []string{"127.0.0.1:9092"} // Replace with your broker addresses
// 	topics := []string{"logs", "critical_logs"}

// 	consumer, err := sarama.NewConsumer(brokers, nil)
// 	if err != nil {
// 		log.Fatalf("Error creating consumer: %v", err)
// 	}
// 	defer consumer.Close()

// 	for _, topic := range topics {
// 		go consumeTopic(consumer, topic)
// 	}

// 	// Prevent the main function from exiting
// 	select {}
// }

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/elastic/go-elasticsearch/v8"
)

type LogEntry struct {
	LogID       float64 `json:"log_id"`
	NodeID      float64 `json:"node_id"`
	LogLevel    string  `json:"log_level"`
	MessageType string  `json:"message_type"`
	Message     string  `json:"message"`
	ServiceName string  `json:"service_name"`
	Timestamp   string  `json:"timestamp"`
}

var esClient *elasticsearch.Client

// Initialize Elasticsearch client
func initElasticsearch() error {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"}, // Replace with your Elasticsearch address
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("error creating Elasticsearch client: %v", err)
	}
	esClient = client
	// Verify connection
	res, err := esClient.Info()
	if err != nil {
		return fmt.Errorf("error connecting to Elasticsearch: %v", err)
	}
	defer res.Body.Close()
	fmt.Println("Elasticsearch initialized:", res)
	return nil
}

// Store log entry in Elasticsearch
func storeLogInElasticsearch(index string, logEntry LogEntry) {
	data, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("Error marshalling log entry: %v", err)
		return
	}

	req := esClient.Index(index, bytes.NewReader(data))
	if req.IsError() {
		log.Printf("Error indexing log: %s", req)
	} else {
		fmt.Printf("Log stored in Elasticsearch index %s: %s\n", index, logEntry.Message)
	}
}

func consumeTopic(consumer sarama.Consumer, topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer for topic %s: %v", topic, err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("Listening for logs on topic: %s...\n", topic)
	var logEntry LogEntry

	for message := range partitionConsumer.Messages() {
		err := json.Unmarshal(message.Value, &logEntry)
		if err != nil {
			log.Printf("Error unmarshalling message from topic %s: %v", topic, err)
			continue
		}

		fmt.Printf("Message received on topic %s: %s, Service: %s\n", topic, string(message.Value), logEntry.ServiceName)

		// Determine Elasticsearch index based on topic
		index := topic
		storeLogInElasticsearch(index, logEntry)
	}
}

func main() {
	// Initialize Elasticsearch
	err := initElasticsearch()
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}

	brokers := []string{"127.0.0.1:9092"} // Replace with your broker addresses
	topics := []string{"logs", "critical_logs"}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	for _, topic := range topics {
		go consumeTopic(consumer, topic)
	}

	// Prevent the main function from exiting
	select {}
}
