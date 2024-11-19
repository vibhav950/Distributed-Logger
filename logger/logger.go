package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/google/uuid"
)

type RegistrationMsg struct {
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

type RegistryMsg struct {
	MessageType string `json:"message_type"`
	NodeID      int    `json:"node_id"`
	ServiceName string `json:"service_name"`
	Status      string `json:"status"`
	Timestamp   string `json:"timestamp"`
}

var globalBrokers = []string{}
var globalTopic = ""

func InitLogger(brokers []string, topic string, createTopic bool) error {
	// Create Kafka topic if it does not exist
	var err error

	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		err = fmt.Errorf("Failed to create Sarama cluster admin: %v\n", err)
		return err
	}
	defer admin.Close()

	if createTopic {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			err = fmt.Errorf("Failed to create Kafka topic: %v\n", err)
			return err
		}
		fmt.Println("Kafka topic created successfully")
	}

	globalBrokers = brokers
	globalTopic = topic
	return nil
}

// func BroadcastLog(log []byte) {
// 	if globalBrokers == nil || globalTopic == "" {
// 		fmt.Println("Logger not initialized")
// 		return
// 	}

// 	// Configure Sarama Kafka producer
// 	config := sarama.NewConfig()
// 	config.Producer.Return.Successes = true
// 	config.Producer.Return.Errors = true

// 	// Create new Kafka producer
// 	producer, err := sarama.NewSyncProducer(globalBrokers, config)
// 	if err != nil {
// 		fmt.Printf("Failed to start Sarama producer: %v\n", err)
// 		return
// 	}
// 	defer producer.Close()

// 	// Create Kafka message
// 	msg := &sarama.ProducerMessage{
// 		Topic: globalTopic,
// 		Value: sarama.ByteEncoder(log),
// 	}

// 	// Send message to Kafka
// 	partition, offset, err := producer.SendMessage(msg)
// 	if err != nil {
// 		fmt.Printf("Failed to send message: %v\n", err)
// 		return
// 	}
// 	fmt.Printf("Message sent to partition %d with offset %d\n", partition, offset)
// }

var fluentdLogger *fluent.Fluent

func InitFluentdLogger(fluentdAddress string) error {
	var err error
	fluentdLogger, err = fluent.New(fluent.Config{
		FluentHost: fluentdAddress,
		FluentPort: 24224,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Fluentd logger: %v", err)
	}
	return nil
}

func BroadcastLog(logData []byte) {
	if fluentdLogger == nil {
		fmt.Println("Fluentd logger not initialized")
		return
	}

	// Decode the JSON data to extract the log level and determine the tag
	var genericLog map[string]interface{}
	err := json.Unmarshal(logData, &genericLog)
	if err != nil {
		fmt.Printf("Failed to parse log data: %v\n", err)
		return
	}

	// Determine tag based on log level or fallback to a default
	tag, ok := genericLog["log_level"].(string)
	if !ok || tag == "" {
		tag = "default"
	}

	// Send log to Fluentd
	err = fluentdLogger.Post(tag, genericLog)
	if err != nil {
		fmt.Printf("Failed to send log to Fluentd: %v\n", err)
	}
}

func GenerateRegistrationMsg(nodeID int, serviceName string) []byte {
	log := RegistrationMsg{
		NodeID:      nodeID,
		MessageType: "REGISTRATION",
		ServiceName: serviceName,
		Timestamp:   time.Now().String(),
	}
	jsonData, _ := json.Marshal(log)
	return jsonData
}

func GenerateInfoLog(nodeID int, serviceName string, message string) []byte {
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
	return jsonData
}

func GenerateWarnLog(nodeID int, serviceName string, message string) []byte {
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
	return jsonData
}

func GenerateErrorLog(nodeID int, serviceName string, message string, errorCode string, errorMessage string) []byte {
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
	return jsonData
}

func GenerateHeartbeatMsg(nodeID int, healthy bool) []byte {
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
	return jsonData
}

func StartHeartbeatRoutine(nodeID int) {
	for {
		BroadcastLog(GenerateHeartbeatMsg(nodeID, true))
		time.Sleep(15 * time.Second)
	}
}

func GenerateRegistryMsg(nodeID int, serviceName string, up bool) []byte {
	var statusString string

	if up {
		statusString = "UP"
	} else {
		statusString = "DOWN"
	}
	registry := RegistryMsg{
		MessageType: "REGISTRATION",
		NodeID:      nodeID,
		ServiceName: serviceName,
		Status:      statusString,
		Timestamp:   time.Now().String(),
	}
	jsonData, _ := json.Marshal(registry)
	return jsonData
}

func DecodeLog(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// func Test() {
// 	/* Set Kafka hostname and topic */
// 	brokers := []string{"localhost:9092"}
// 	topic := "logs"

// 	err := InitLogger(brokers, topic, false)
// 	if err != nil {
// 		fmt.Printf("%v", err)
// 		return
// 	}

// 	InitFluentdLogger("localhost")

// 	// Produce logs
// 	BroadcastLog(GenerateRegistrationMsg(1, "foo_service"))

// 	BroadcastLog(GenerateInfoLog(1, "foo_service", "This is an info message"))

// 	BroadcastLog(GenerateWarnLog(1, "foo_service", "This is a warning message"))

// 	BroadcastLog(GenerateErrorLog(1, "foo_service", "This is an error message", "500", "Internal Server Error"))

// 	BroadcastLog(GenerateHeartbeatMsg(1, true))

// 	fmt.Println("Logs sent successfully")

// 	// Decode example
// 	var decodedLog InfoLog
// 	infoLog := GenerateInfoLog(1, "foo_service", "This is an info message")
// 	err = DecodeLog(infoLog, &decodedLog)
// 	if err != nil {
// 		fmt.Printf("Failed to decode log: %v\n", err)
// 	} else {
// 		fmt.Printf("Decoded log: %+v\n", decodedLog)
// 	}
// }

func Test() {
	// Initialize Fluentd logger
	err := InitFluentdLogger("localhost")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	// Produce logs
	BroadcastLog(GenerateInfoLog(1, "foo_service", "This is an info message"))
	BroadcastLog(GenerateWarnLog(1, "foo_service", "This is a warning message"))

	fmt.Println("Logs sent successfully")
}

func main() {
	Test()
}
