package logger

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

var globalBrokers = []string{"localhost:9092"}
var globalTopic = ""
var globalProducer sarama.SyncProducer = nil
var globalFluentdLogger *fluent.Fluent = nil

func CHECK(err error) {
	if err != nil {
		panic(err)
	}
}

func initFluentdLogger(fluentdAddress string) error {
	var err error
	globalFluentdLogger, err = fluent.New(fluent.Config{
		FluentHost: fluentdAddress,
		FluentPort: 24224,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Fluentd logger: %v", err)
	}
	return nil
}

func InitLogger(kafkaBrokers []string, kafkaCriticalTopic string, fluentdAddress string) error {
	var err error
	var producer sarama.SyncProducer

	// Create kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err = sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return fmt.Errorf("Failed to create Sarama producer: %v\n", err)
	}

	err = initFluentdLogger(fluentdAddress)
	if err != nil {
		return err
	}

	globalBrokers = kafkaBrokers
	globalTopic = kafkaCriticalTopic
	globalProducer = producer
	return nil
}

func CloseLogger() {
	if globalProducer != nil {
		globalProducer.Close()
	}
	if globalFluentdLogger != nil {
		globalFluentdLogger.Close()
	}
	globalProducer = nil
	globalFluentdLogger = nil
}

func broadcastLogNow(log []byte) error {
	if globalBrokers == nil || globalTopic == "" {
		return fmt.Errorf("Logger not initialized. Please call InitLogger first")
	}

	if globalProducer == nil {
		return fmt.Errorf("Logger not initialized. Please call InitLogger first")
	}

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: globalTopic,
		Value: sarama.ByteEncoder(log),
	}

	// Send message to Kafka
	partition, offset, err := globalProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("Failed to send message: %v\n", err)
	}

	fmt.Println("[DEBUG] Message sent to partition", partition, "with offset", offset, "on topic", globalTopic)
	return nil
}

func broadcastLog(logData []byte) error {
	if globalFluentdLogger == nil {
		return fmt.Errorf("Fluentd logger not initialized. Please call InitLogger first")
	}

	// Decode the JSON data to extract the log level and determine the tag
	var genericLog map[string]interface{}
	err := json.Unmarshal(logData, &genericLog)
	if err != nil {
		return fmt.Errorf("Failed to parse log data: %v\n", err)
	}

	// Determine tag based on log level or fallback to a default
	tag, ok := genericLog["log_level"].(string)
	if !ok || tag == "" {
		tag = "default"
	}

	// Send log to Fluentd
	err = globalFluentdLogger.Post(tag, genericLog)
	if err != nil {
		return fmt.Errorf("Failed to send log to Fluentd: %v\n", err)
	}

	fmt.Println("[DEBUG] Log sent to Fluentd with tag", tag)
	return nil
}

func SendRegistrationMsg(nodeID int, serviceName string) {
	log := RegistrationMsg{
		NodeID:      nodeID,
		MessageType: "REGISTRATION",
		ServiceName: serviceName,
		Timestamp:   time.Now().String(),
	}
	jsonData, _ := json.Marshal(log)
	CHECK(broadcastLogNow(jsonData))
}

func SendInfoLog(nodeID int, serviceName string, message string) {
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
	CHECK(broadcastLog(jsonData))
}

func SendWarnLog(nodeID int, serviceName string, message string) {
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
	CHECK(broadcastLogNow(jsonData))
}

func SendErrorLog(nodeID int, serviceName string, message string, errorCode string, errorMessage string) {
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
	CHECK(broadcastLogNow(jsonData))
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
		broadcastLogNow(GenerateHeartbeatMsg(nodeID, true))
		time.Sleep(15 * time.Second)
		fmt.Println("[DEBUG] Heartbeat sent")
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

func Test() {
	/* Initialize the logger */
	err := InitLogger(globalBrokers, "critical_logs", "localhost")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	SendErrorLog(1, "foo_service", "This is an error message", "500", "Internal Server Error")
	SendWarnLog(1, "foo_service", "This is a warning message")

	// Produce logs
	SendInfoLog(1, "foo_service", "This is an info message")
	SendInfoLog(1, "foo_service", "This is an info message")

	fmt.Println("Logs sent successfully")
}

// func main() {
// 	Test()
// }
