package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	brokerRESTURL = "http://localhost:9000/TOPIC/demo/tracing/topic"
	otelCollector = "http://localhost:4318/v1/traces"
	username      = "default"
	password      = "default"
	numMessage    = 5
	serviceName   = "solace-rest-publisher"
)

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func buildTraceparent(traceID, spanID string, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", traceID, spanID, flags)
}

func sendSpanToCollector(traceID, spanID string, startNano, endNano int64, topic string) error {
	payload := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]string{"stringValue": serviceName}},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"spans": []map[string]interface{}{
							{
								"traceId":           traceID,
								"spanId":            spanID,
								"name":              topic + " publish",
								"kind":              3,
								"startTimeUnixNano": fmt.Sprintf("%d", startNano),
								"endTimeUnixNano":   fmt.Sprintf("%d", endNano),
								"status":            map[string]interface{}{"code": 1},
								"attributes": []map[string]interface{}{
									{"key": "messaging.system", "value": map[string]string{"stringValue": "solace"}},
									{"key": "messaging.destination.name", "value": map[string]string{"stringValue": topic}},
									{"key": "messaging.operation", "value": map[string]string{"stringValue": "publish"}},
								},
							},
						},
					},
				},
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(otelCollector, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody := make([]byte, 1024)
	n, _ := resp.Body.Read(respBody)
	fmt.Printf("  → OTel Collector response: %s - %s\n", resp.Status, string(respBody[:n]))
	return nil
}

func main() {
	client := &http.Client{}

	for i := 1; i <= numMessage; i++ {
		traceID := randomHex(16)
		spanID := randomHex(8)
		traceparent := buildTraceparent(traceID, spanID, true)
		payload := fmt.Sprintf("Hello from REST publisher! Message #%d", i)

		startNano := time.Now().UnixNano()

		req, err := http.NewRequest(http.MethodPost, brokerRESTURL, bytes.NewBufferString(payload))
		if err != nil {
			fmt.Printf("Failed to create request: %v\n", err)
			continue
		}
		req.SetBasicAuth(username, password)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("traceparent", traceparent)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Failed to send: %v\n", err)
			continue
		}

		// Debug: print semua response headers dari broker
		fmt.Println("  → Response headers dari broker:")
		for k, v := range resp.Header {
			fmt.Printf("     %s: %s\n", k, v)
		}
		resp.Body.Close()

		endNano := time.Now().UnixNano()

		fmt.Printf("Message #%d - traceparent: %s, HTTP: %s\n", i, traceparent, resp.Status)

		if err := sendSpanToCollector(traceID, spanID, startNano, endNano, "demo/tracing/topic"); err != nil {
			fmt.Printf("  → Failed to send span: %v\n", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Done!")
}