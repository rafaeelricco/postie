package debezium

import (
	"io"
	"net/http"
	"strings"
)

// maxErrorBody bounds how much of a Connect error response is kept.
const maxErrorBody = 64 * 1024

// connectorURL is the Kafka Connect REST path of one connector resource.
//
//	connectorURL("http://connect:8083/", "c1", "status") // http://connect:8083/connectors/c1/status
func connectorURL(connectURL, connector, resource string) string {
	return strings.TrimRight(connectURL, "/") + "/connectors/" + connector + "/" + resource
}

// do sends the request with client, or with http.DefaultClient when nil.
func do(client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

// errorBody reads a bounded prefix of a response body for an error message.
func errorBody(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	return string(body)
}
