package logger

import (
	"encoding/json"
	"fmt"
	"time"

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

func BroadcastLog(log string) {
	fmt.Println(log) //Implement UDP broadcast here
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

func Test() {
	fmt.Println(GenerateRegistrationLog(1, "foo_service"))
	fmt.Println(GenerateInfoLog(1, "foo_service", "This is an info message"))
	fmt.Println(GenerateWarnLog(1, "foo_service", "This is a warning message"))
	fmt.Println(GenerateErrorLog(1, "foo_service", "This is an error message", "500", "Internal Server Error"))
	fmt.Println(GenerateHeartbeat(1, true))
}
