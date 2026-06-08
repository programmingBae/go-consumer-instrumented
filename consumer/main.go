package main

// consumer/main.go
// Simple consumer WITHOUT distributed tracing instrumentation.
// Tujuan: receive pesan dari Solace broker di localhost.

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

const (
	brokerURL = "tcp://localhost:55554"
	vpnName   = "default"
	username  = "default"
	password  = "default"
	topicName = "demo/tracing/topic"
	queueName = "demo-queue"
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
	// STEP 3: Buat durable queue subscription
	// Queue harus sudah exist di broker (buat via Solace Manager)
	// dengan subscription topic: demo/tracing/topic
	// ──────────────────────────────────────────────
	queue := resource.QueueDurableNonExclusive(queueName)

	// ──────────────────────────────────────────────
	// STEP 4: Buat persistent message receiver
	// ──────────────────────────────────────────────
	receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
		WithMessageAutoAcknowledgement(). // Auto-ack setelah handler selesai
		Build(queue)
	if err != nil {
		fmt.Printf("Failed to build receiver: %v\n", err)
		os.Exit(1)
	}

	if err := receiver.Start(); err != nil {
		fmt.Printf("Failed to start receiver: %v\n", err)
		os.Exit(1)
	}
	defer receiver.Terminate(0)

	// ──────────────────────────────────────────────
	// STEP 5: Definisikan message handler
	// ──────────────────────────────────────────────
	var messageHandler solace.MessageHandler = func(msg message.InboundMessage) {
		var body string
		if payload, ok := msg.GetPayloadAsString(); ok {
			body = payload
		} else if payload, ok := msg.GetPayloadAsBytes(); ok {
			body = string(payload)
		}
		fmt.Printf("📩 Received message from topic [%s]: %s\n",
			msg.GetDestinationName(), body)
	}

	// ──────────────────────────────────────────────
	// STEP 6: Register handler dan mulai receive
	// ──────────────────────────────────────────────
	if err := receiver.ReceiveAsync(messageHandler); err != nil {
		fmt.Printf("Failed to register handler: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("👂 Listening on queue [%s] (topic: %s)...\n", queueName, topicName)
	fmt.Println("Press Ctrl+C to exit")

	// Tunggu sinyal interrupt untuk graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 Shutting down consumer...")
}
