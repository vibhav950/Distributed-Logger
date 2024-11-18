package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type RegistrationLog struct {
	NodeID      int    `json:"node_id"`
	MessageType string `json:"message_type"`
	ServiceName string `json:"service_name"`
	Timestamp   string `json:"timestamp"`
}

type InfoLog struct {
	LogID       int    `json:"log_id"`
	NodeID      int    `json:"node_id"`
	LogLevel    string `json:"log_level"`
	MessageType string `json:"message_type"`
	Message     string `json:"message"`
	ServiceName string `json:"service_name"`
	Timestamp   string `json:"timestamp"`
}

type WarnLog struct {
	LogID            int    `json:"log_id"`
	NodeID           int    `json:"node_id"`
	LogLevel         string `json:"log_level"`
	MessageType      string `json:"message_type"`
	Message          string `json:"message"`
	ServiceName      string `json:"service_name"`
	ResponseTimeMs   string `json:"response_time_ms"`
	ThresholdLimitMs string `json:"threshold_limit_ms"`
	Timestamp        string `json:"timestamp"`
}

type ErrorLog struct {
	LogID        int    `json:"log_id"`
	NodeID       int    `json:"node_id"`
	LogLevel     string `json:"log_level"`
	MessageType  string `json:"message_type"`
	Message      string `json:"message"`
	ServiceName  string `json:"service_name"`
	ErrorDetails struct {
		ErrorCode    string `json:"error_code"`
		ErrorMessage string `json:"error_message"`
	} `json:"error_details"`
	Timestamp string `json:"timestamp"`
}

type Heartbeat struct {
	NodeID      int    `json:"node_id"`
	MessageType string `json:"message_type"`
	Status      string `json:"status"`
	Timestamp   string `json:"timestamp"`
}

func BroadcastLog(log string, topic string, brokers []string) {
	// Configure Sarama Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Printf("Failed to start Sarama producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(log),
	}

	// Send the message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
		return
	}

	fmt.Printf("Message sent to partition %d with offset %d\n", partition, offset)
}

func GenerateRegistrationLog(nodeID int, serviceName string) string {
	log := RegistrationLog{
		NodeID:      nodeID,
		MessageType: "REGISTRATION",
		ServiceName: serviceName,
		Timestamp:   time.Now().String(),
	}

	jsonData, _ := json.Marshal(log)
	return string(jsonData)
}

func GenerateInfoLog(nodeID int, serviceName string, message string) string {
	log := InfoLog{
		LogID:       int(uuid.New().ID()),
		NodeID:      nodeID,
		LogLevel:    "INFO",
		MessageType: "LOG",
		Message:     message,
		ServiceName: serviceName,
		Timestamp:   time.Now().String(),
	}

	jsonData, _ := json.Marshal(log)
	return string(jsonData)
}

func GenerateWarnLog(nodeID int, serviceName string, message string) string {
	log := WarnLog{
		LogID:            int(uuid.New().ID()),
		NodeID:           nodeID,
		LogLevel:         "WARN",
		MessageType:      "LOG",
		Message:          message,
		ServiceName:      serviceName,
		ResponseTimeMs:   "",
		ThresholdLimitMs: "",
		Timestamp:        time.Now().String(),
	}

	jsonData, _ := json.Marshal(log)
	return string(jsonData)
}

func GenerateErrorLog(nodeID int, serviceName string, message string, errorCode string, errorMessage string) string {
	log := ErrorLog{
		LogID:       int(uuid.New().ID()),
		NodeID:      nodeID,
		LogLevel:    "ERROR",
		MessageType: "LOG",
		Message:     message,
		ServiceName: serviceName,
		ErrorDetails: struct {
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		}{
			ErrorCode:    errorCode,
			ErrorMessage: errorMessage,
		},
		Timestamp: time.Now().String(),
	}

	jsonData, _ := json.Marshal(log)
	return string(jsonData)
}

func GenerateHeartbeat(nodeID int, healthy bool) string {
	status := "UP"
	if !healthy {
		status = "DOWN"
	}

	heartbeat := Heartbeat{
		NodeID:      nodeID,
		MessageType: "HEARTBEAT",
		Status:      status,
		Timestamp:   time.Now().String(),
	}

	jsonData, _ := json.Marshal(heartbeat)
	return string(jsonData)
}

func main() {
	brokers := []string{"192.168.239.251:9092"} // Replace with your Kafka broker addresses
	topic := "logs"                             // Kafka topic for logs

	log := GenerateRegistrationLog(1, "foo_service")
	BroadcastLog(log, topic, brokers)

	log = GenerateInfoLog(1, "foo_service", "This is an info message")
	BroadcastLog(log, topic, brokers)

	log = GenerateWarnLog(1, "foo_service", "This is a warning message")
	BroadcastLog(log, topic, brokers)

	log = GenerateErrorLog(1, "foo_service", "This is an error message", "500", "Internal Server Error")
	BroadcastLog(log, topic, brokers)

	log = GenerateHeartbeat(1, true)
	BroadcastLog(log, topic, brokers)
}
