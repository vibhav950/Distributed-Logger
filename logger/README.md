# Logger Package

This package provides logging functionalities using Kafka and Fluentd. It supports different log levels and message types.

## Functions

### `CHECK(err error)`

Checks if an error occurred and panics if it did.

### `initFluentdLogger(fluentdAddress string) error`

Initializes the Fluentd logger.

- **Parameters:**
  - `fluentdAddress`: The address of the Fluentd server.
- **Returns:** An error if initialization fails.

### `InitLogger(kafkaBrokers []string, kafkaCriticalTopic string, fluentdAddress string) error`

Initializes the logger with Kafka and Fluentd.

- **Parameters:**
  - `kafkaBrokers`: A list of Kafka broker addresses.
  - `kafkaCriticalTopic`: The Kafka topic for critical logs.
  - `fluentdAddress`: The address of the Fluentd server.
- **Returns:** An error if initialization fails.

### `CloseLogger()`

Closes the Kafka producer and Fluentd logger.

### `broadcastLogNow(log []byte) error`

Broadcasts a log message to Kafka immediately.

- **Parameters:**
  - `log`: The log message in byte format.
- **Returns:** An error if broadcasting fails.

### `broadcastLog(logData []byte) error`

Broadcasts a log message to Fluentd.

- **Parameters:**
  - `logData`: The log message in byte format.
- **Returns:** An error if broadcasting fails.

### `SendRegistrationMsg(nodeID int, serviceName string)`

Sends a registration message.

- **Parameters:**
  - `nodeID`: The ID of the node.
  - `serviceName`: The name of the service.

### `SendInfoLog(nodeID int, serviceName string, message string)`

Sends an info log message.

- **Parameters:**
  - `nodeID`: The ID of the node.
  - `serviceName`: The name of the service.
  - `message`: The log message.

### `SendWarnLog(nodeID int, serviceName string, message string)`

Sends a warning log message.

- **Parameters:**
  - `nodeID`: The ID of the node.
  - `serviceName`: The name of the service.
  - `message`: The log message.

### `SendErrorLog(nodeID int, serviceName string, message string, errorCode string, errorMessage string)`

Sends an error log message.

- **Parameters:**
  - `nodeID`: The ID of the node.
  - `serviceName`: The name of the service.
  - `message`: The log message.
  - `errorCode`: The error code.
  - `errorMessage`: The error message.

### `generateHeartbeatMsg(nodeID int, healthy bool) []byte`

Generates a heartbeat message.

- **Parameters:**
  - `nodeID`: The ID of the node.
  - `healthy`: The health status of the node.
- **Returns:** The heartbeat message in byte format.

### `StartHeartbeatRoutine(nodeID int)`

Starts a routine to send heartbeat messages every 15 seconds.

- **Parameters:**
  - `nodeID`: The ID of the node.
