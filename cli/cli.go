package main

import (
	"bytes"
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
		Username:  "elastic",              // Add your username here
		Password:  "XXiqV27E1FcB*Qeh0jox", // Add your password here
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

// DeleteAllDocuments deletes all documents from the index
func (ec *ElasticClient) DeleteAllDocuments() error {
	// Create a delete-by-query request
	query := `{
		"query": {
			"match_all": {}
		}
	}`

	res, err := ec.Client.DeleteByQuery([]string{ec.Index}, bytes.NewReader([]byte(query)))
	if err != nil {
		return fmt.Errorf("error deleting documents: %v", err)
	}
	defer res.Body.Close()

	fmt.Println("All documents deleted successfully.")
	return nil
}

// ShowLogs fetches logs based on the specified level and prints them
func (ec *ElasticClient) ShowLogs(level string) {
	var query string
	if level == "info" {
		// Query for info level logs
		query = `{
			"query": {
				"match": {
					"log_level": "INFO"
				}
			}
		}`
	} else if level == "alerts" {
		// Query for warn and error level logs (alerts)
		query = `{
					"query": {
						"bool": {
						"should": [
							{ "match": { "log_level": "ERROR" } },
							{ "match": { "log_level": "WARN" } }
						]
						}
					}
				}`
	} else {
		fmt.Println("Invalid log level specified. Use 'info' or 'alerts'.")
		return
	}

	logs, err := ec.SearchLogs(query)
	if err != nil {
		log.Fatalf("Failed to retrieve logs: %v", err)
	}

	// Print the search results in a pretty format
	hits := logs["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		logData := hit.(map[string]interface{})["_source"].(map[string]interface{})
		// Safely access the "level" and "message" fields
		level, levelOk := logData["log_level"].(string)
		message, messageOk := logData["message"].(string)

		// Handle missing fields gracefully
		if !levelOk {
			level = "unknown"
		}
		if !messageOk {
			message = "No message"
		}

		// Print the log with safe defaults
		fmt.Printf("Level: %s - Message: %s\n", level, message)
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
				Usage: "Show logs based on the specified log level (info or alerts)",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "level",
						Usage:    "Specify the log level to filter by: 'info' or 'alerts'",
						Required: true,
					},
					&cli.BoolFlag{
						Name:     "delete",
						Usage:    "Delete all documents from the Elasticsearch index",
						Required: false,
					},
				},
				Action: func(c *cli.Context) error {
					index := c.String("index")
					level := c.String("level")
					deleteFlag := c.Bool("delete")

					// Initialize the Elasticsearch client
					ec, err := NewElasticClient(index)
					if err != nil {
						return fmt.Errorf("failed to initialize Elasticsearch client: %w", err)
					}

					// If the delete flag is set, delete all documents
					if deleteFlag {
						err := ec.DeleteAllDocuments()
						if err != nil {
							return fmt.Errorf("failed to delete documents: %w", err)
						}
						return nil
					}

					// Show logs based on the level
					ec.ShowLogs(level)

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
