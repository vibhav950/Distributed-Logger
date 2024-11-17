package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// Config for Elasticsearch client
var cfg = elasticsearch.Config{
	Addresses: []string{
		"https://localhost:9200", // Use HTTPS
	},
	Username: "elastic",              // Replace with your username
	Password: "a+4JEIQuMUa2rVZnxJ*v", // Replace with your password
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Disable SSL verification (only for development)
		},
	},
}

// Enhanced logging function
func logResponse(res *esapi.Response) {
	log.Printf("Status Code: %d", res.StatusCode)
	if res.Body != nil {
		defer res.Body.Close()
		var responseBody map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&responseBody); err == nil {
			log.Printf("Response Body: %v", responseBody)
		} else {
			log.Printf("Error decoding response body: %v", err)
		}
	}
}

func getClient() *elasticsearch.Client {

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}
	log.Println("Elasticsearch client initialized with HTTPS and authentication")
	return es
}

func indexDocument(es *elasticsearch.Client, index string, documentID string, doc map[string]interface{}) {
	log.Println("Indexing document...")
	data, _ := json.Marshal(doc)
	log.Printf("Index: %s, Document ID: %s, Payload: %s", index, documentID, string(data))

	res, err := es.Index(
		index,
		bytes.NewReader(data),
		es.Index.WithDocumentID(documentID),
		es.Index.WithRefresh("true"),
	)
	if err != nil {
		log.Fatalf("Error indexing document: %s", err)
	}
	logResponse(res)
}

func search(es *elasticsearch.Client, index string, query map[string]interface{}) {
	log.Println("Searching documents...")
	data, _ := json.Marshal(query)
	log.Printf("Index: %s, Query: %s", index, string(data))

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index),
		es.Search.WithBody(strings.NewReader(string(data))),
		es.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		log.Fatalf("Error searching documents: %s", err)
	}
	logResponse(res)
}

func deleteDocument(es *elasticsearch.Client, index string, documentID string) {
	log.Println("Deleting document...")
	log.Printf("Index: %s, Document ID: %s", index, documentID)

	res, err := es.Delete(
		index,
		documentID,
		es.Delete.WithRefresh("true"),
	)
	if err != nil {
		log.Fatalf("Error deleting document: %s", err)
	}
	logResponse(res)
}

func main() {
	es := getClient()

	// Index a document
	doc := map[string]interface{}{
		"title":   "Go and Elasticsearch",
		"content": "A simple example of using Elasticsearch with Go.",
	}
	indexDocument(es, "my-index", "1", doc)

	// Search documents
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "Go",
			},
		},
	}
	search(es, "my-index", query)

	// Delete a document
	deleteDocument(es, "my-index", "1")
}
