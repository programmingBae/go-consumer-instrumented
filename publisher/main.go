package main

// publisher/main.go
// Simple publisher WITHOUT distributed tracing instrumentation.
// Tujuan: kirim pesan ke Solace broker di localhost.

import (
	"fmt"
	"os"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
)

const (
	brokerURL  = "tcp://localhost:55554"
	vpnName    = "default"
	username   = "default"
	password   = "default"
	topicName  = "demo/tracing/topic"
	numMessage = 5
)

func main() {
	// ──────────────────────────────────────────────
	// STEP 1: Konfigurasi koneksi ke Solace broker
	// ──────────────────────────────────────────────
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                brokerURL,
		config.ServicePropertyVPNName:                   vpnName,
		config.AuthenticationPropertySchemeBasicUserName: username,
		config.AuthenticationPropertySchemeBasicPassword: password,
	}

	// ──────────────────────────────────────────────
	// STEP 2: Build dan connect messaging service
	// ──────────────────────────────────────────────
	messagingService, err := messaging.NewMessagingServiceBuilder().
		FromConfigurationProvider(brokerConfig).
		Build()
	if err != nil {
		fmt.Printf("Failed to build messaging service: %v\n", err)
		os.Exit(1)
	}

	if err := messagingService.Connect(); err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer messagingService.Disconnect()
	fmt.Println("✅ Connected to Solace broker")

	// ──────────────────────────────────────────────
	// STEP 3: Buat persistent message publisher
	// ──────────────────────────────────────────────
	publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
	if err != nil {
		fmt.Printf("Failed to build publisher: %v\n", err)
		os.Exit(1)
	}

	if err := publisher.Start(); err != nil {
		fmt.Printf("Failed to start publisher: %v\n", err)
		os.Exit(1)
	}
	defer publisher.Terminate(5 * time.Second)

	// ──────────────────────────────────────────────
	// STEP 4: Build dan publish pesan
	// ──────────────────────────────────────────────
	msgBuilder := messagingService.MessageBuilder()

	for i := 1; i <= numMessage; i++ {
		payload := fmt.Sprintf("Hello from publisher! Message #%d", i)

		msg, err := msgBuilder.
			WithProperty("messageIndex", fmt.Sprintf("%d", i)).
			BuildWithStringPayload(payload)
		if err != nil {
			fmt.Printf("Failed to build message: %v\n", err)
			continue
		}

		topic := resource.TopicOf(topicName)
		if err := publisher.Publish(msg, topic, nil, nil); err != nil {
			fmt.Printf("Failed to publish message: %v\n", err)
			continue
		}

		fmt.Printf("📤 Published: %s\n", payload)

		// Hindari publish terlalu cepat
		time.Sleep(500 * time.Millisecond)
	}

	// Flush semua pesan sebelum disconnect
	lastMsg, _ := msgBuilder.BuildWithStringPayload("LAST_MESSAGE")
	_ = publisher.PublishAwaitAcknowledgement(
		lastMsg,
		resource.TopicOf(topicName),
		5*time.Second,
		nil,
	)

	fmt.Println("✅ All messages published!")
}