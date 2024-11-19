package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/urfave/cli/v2"
)

// ElasticClient wraps the Elasticsearch client for querying logs
type ElasticClient struct {
	Client *elasticsearch.Client
	Index  string
}

// NewElasticClient initializes an Elasticsearch client
func NewElasticClient(index string) (*ElasticClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Username:  "elastic",
		Password:  "XXiqV27E1FcB*Qeh0jox",
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ElasticClient{Client: client, Index: index}, nil
}

// SearchLogs performs a search on the Elasticsearch index with a query
func (ec *ElasticClient) SearchLogs(query string) (map[string]interface{}, error) {
	searchRes, err := ec.Client.Search(
		ec.Client.Search.WithIndex(ec.Index),
		ec.Client.Search.WithBody(strings.NewReader(query)),
		ec.Client.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}
	defer searchRes.Body.Close()

	var searchResult map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("failed to decode search result: %w", err)
	}

	return searchResult, nil
}

// ShowLogs fetches logs based on the specified level and prints them
func (ec *ElasticClient) ShowLogs(level string, limit int) {
	var query string
	fmt.Println(level, limit)

	if level == "all" {
		query = fmt.Sprintf(`{
			"query": {
				"match_all": {}
			},
			"size": %d
		}`, limit)
	} else if level == "info" {
		query = fmt.Sprintf(`{
			"query": {
				"match": {
					"log_level": "%s"
				}
			},
			"size": %d
		}`, level, limit)
	} else {
		query = fmt.Sprintf(`{
			"query": {
				"bool": {
					"should": [
					{
						"terms": {
						"log_level.keyword": ["WARN", "ERROR"]
						}
					},
					{
						"terms": {
						"message_type.keyword": ["REGISTRATION", "HEARTBEAT"]
						}
					}
					],
					"minimum_should_match": 1
				}
				}
				,
			"size": %d
		}`, limit)
	}

	logs, err := ec.SearchLogs(query)
	if err != nil {
		log.Fatalf("Failed to retrieve logs: %v", err)
	}

	hits := logs["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		logData := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if logData["message_type"] != nil {
			messageType := logData["message_type"].(string)

			switch messageType {
			case "LOG":
				{
					level, levelOk := logData["log_level"].(string)
					message, messageOk := logData["message"].(string)

					if !levelOk {
						level = "unknown"
					}
					if !messageOk {
						message = "No message"
					}

					fmt.Printf("%s - Message: %s\n", level, message)
				}
			case "HEARTBEAT":
				{
					fmt.Printf("%s - id: %d - status: %s\n", messageType, int(logData["node_id"].(float64)), logData["status"].(string))
				}
			case "REGISTRATION":
				{
					fmt.Printf("%s - id: %d - service name: %s\n", messageType, int(logData["node_id"].(float64)), logData["service_name"].(string))
				}
			}
		}
	}
}

func main() {
	app := &cli.App{
		Name:  "Log CLI",
		Usage: "Search and display logs from Elasticsearch",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "index",
				Value:    "kafka-logs",
				Usage:    "Elasticsearch index to query",
				Required: false,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "logs",
				Usage: "Show logs based on the specified log level (info, alerts, or all)",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "level",
						Usage:    "Specify the log level to filter by: 'info', 'alerts', or 'all'",
						Required: false,
					},
					&cli.IntFlag{
						Name:     "limit",
						Usage:    "Specify the number of logs to retrieve",
						Value:    10, // Default limit
						Required: false,
					},
				},
				Action: func(c *cli.Context) error {
					index := c.String("index")
					level := c.String("level")
					limit := c.Int("limit")

					if limit <= 0 {
						return fmt.Errorf("limit must be a positive number")
					}

					if level == "" {
						fmt.Println("Please specify a valid log level using --level.")
						return nil
					}

					ec, err := NewElasticClient(index)
					if err != nil {
						return fmt.Errorf("failed to initialize Elasticsearch client: %w", err)
					}

					if level == "info" || level == "alerts" || level == "all" {
						ec.ShowLogs(level, limit)
						return nil
					}

					fmt.Println("Invalid log level. Use 'info', 'alerts', or 'all'.")
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
