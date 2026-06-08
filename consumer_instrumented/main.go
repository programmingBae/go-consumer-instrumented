package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	solaceotel "solace.dev/go/messaging-trace/opentelemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otel_propagation "go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.19.0"
	"go.opentelemetry.io/otel/trace"
)

func getEnv(key string) string {
	return os.Getenv(key)
}

func main() {
	ctx := context.Background()

	serviceName := getEnv("OTEL_SERVICE_NAME")
	brokerURL   := getEnv("SOLACE_BROKER_URL")
	vpnName     := getEnv("SOLACE_VPN")
	username    := getEnv("SOLACE_USERNAME")
	password    := getEnv("SOLACE_PASSWORD")
	queueName   := getEnv("SOLACE_QUEUE")

	// ══════════════════════════════════════════════════════════════════════
	// [TRACING STEP 1] Setup OpenTelemetry TracerProvider
	// ══════════════════════════════════════════════════════════════════════
	// Kirim span ke OTEL Collector via gRPC (port 4317).
	// Endpoint dibaca otomatis dari env: OTEL_EXPORTER_OTLP_ENDPOINT
	// Collector yang forward ke Grafana Tempo.
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(), // hapus kalau collector pakai TLS
	)
	if err != nil {
		fmt.Printf("Failed to create exporter: %v\n", err)
		os.Exit(1)
	}

	res, _ := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down tracer provider: %v\n", err)
		}
	}()

	// ══════════════════════════════════════════════════════════════════════
	// [TRACING STEP 2] Register global TracerProvider & TextMapPropagator
	// ══════════════════════════════════════════════════════════════════════
	// W3C TraceContext → format "traceparent" yang dipakai Solace broker.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(otel_propagation.TraceContext{})

	fmt.Printf("✅ OpenTelemetry initialized (service: %s)\n", serviceName)

	// ══════════════════════════════════════════════════════════════════════
	// Setup Solace Messaging
	// ══════════════════════════════════════════════════════════════════════
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost:                brokerURL,
		config.ServicePropertyVPNName:                   vpnName,
		config.AuthenticationPropertySchemeBasicUserName: username,
		config.AuthenticationPropertySchemeBasicPassword: password,
	}

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

	queue := resource.QueueDurableNonExclusive(queueName)
	receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
		WithMessageAutoAcknowledgement().
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

	// ══════════════════════════════════════════════════════════════════════
	// [TRACING STEP 3–7] Message Handler dengan Distributed Tracing
	// ══════════════════════════════════════════════════════════════════════
	var messageHandler solace.MessageHandler = func(msg message.InboundMessage) {

		// ── [TRACING STEP 3] Buat InboundMessageCarrier ───────────────────
		// Adapter antara Solace InboundMessage dan OTel TextMapCarrier.
		// Carrier ini tahu cara membaca "traceparent" dari Solace message
		// user properties (format W3C TraceContext).
		inboundCarrier := solaceotel.NewInboundMessageCarrier(msg)

		// ── [TRACING STEP 4] Extract parent SpanContext dari pesan ─────────
		// Extract() membaca "traceparent" dari carrier → dapatkan SpanContext
		// upstream (broker). Span yang dibuat dari context ini akan terhubung
		// ke trace yang sama (TraceID identik).
		// Jika pesan tidak ada trace context → span menjadi root span baru.
		parentCtx := otel.GetTextMapPropagator().Extract(
			context.Background(),
			inboundCarrier,
		)

		// ── [TRACING STEP 5] Definisikan span attributes ──────────────────
		attrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("PubSub+"),
			semconv.MessagingDestinationNameKey.String(msg.GetDestinationName()),
			semconv.MessagingOperationReceive,
			attribute.String("messaging.consumer.queue", queueName),
		}
		spanOpts := []trace.SpanStartOption{
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		}

		// ── [TRACING STEP 6] Mulai receive span sebagai child ─────────────
		tracer := otel.GetTracerProvider().Tracer(serviceName)
		_, span := tracer.Start(
			parentCtx,
			fmt.Sprintf("%s receive", msg.GetDestinationName()),
			spanOpts...,
		)

		// ── [TRACING STEP 7] Tutup span dengan defer ──────────────────────
		defer span.End()

		// ══════════════════════════════════════════════════════════════════
		// Business logic
		// ══════════════════════════════════════════════════════════════════
		var body string
		if payload, ok := msg.GetPayloadAsString(); ok {
			body = payload
		} else if payload, ok := msg.GetPayloadAsBytes(); ok {
			body = string(payload)
		}

		fmt.Printf("📩 [%s] %s\n", msg.GetDestinationName(), body)
		fmt.Printf("   TraceID : %s\n", span.SpanContext().TraceID())
		fmt.Printf("   SpanID  : %s\n", span.SpanContext().SpanID())
		fmt.Printf("   IsRemote: %v\n", span.SpanContext().IsRemote())
	}

	if err := receiver.ReceiveAsync(messageHandler); err != nil {
		fmt.Printf("Failed to register handler: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("👂 Listening on queue [%s] with distributed tracing...\n", queueName)
	fmt.Println("Press Ctrl+C to exit")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("\n🛑 Shutting down consumer...")
}