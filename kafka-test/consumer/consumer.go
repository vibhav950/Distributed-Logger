package main

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
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

func main() {
	brokers := []string{"172.16.172.19:9092"} // Replace "localhost" with "<laptop1-IP>" if needed
	topic := "logs"

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Listening for logs...")
	var infoLog LogEntry

	for message := range partitionConsumer.Messages() {
		_ = json.Unmarshal([]byte(message.Value), &infoLog)
		fmt.Println("Message received: ", string(message.Value), infoLog.ServiceName)
	}
}
