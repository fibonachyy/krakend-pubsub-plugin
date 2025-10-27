package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/luraproject/lura/v2/logging"
)

const (
	pluginName      = "krakend-pubsub-plugin"
	pluginNamespace = "plugin/http-client"
)

// Exported variable KrakenD loader expects
var ClientRegisterer registerer = pluginName

type registerer string

var logger logging.Logger

// Logger loader
func (r registerer) RegisterLogger(l interface{}) {
	lg, ok := l.(logging.Logger)
	if !ok {
		return
	}
	logger = lg
	logger.Debug(fmt.Sprintf("[PLUGIN: %s] Logger loaded", r))
}

// RegisterClients is called by KrakenD to register your plugin
func (r registerer) RegisterClients(f func(
	name string,
	handler func(ctx context.Context, extra map[string]interface{}) (http.Handler, error),
)) {
	f(string(r), r.registerClients)
}

// Publisher config
type publisherCfg struct {
	TopicURL string `json:"topic_url,omitempty"`
}

// Actual HTTP handler for KrakenD backend
func (r registerer) registerClients(_ context.Context, extra map[string]interface{}) (http.Handler, error) {

	fmt.Println(extra)
	cfg := parseExtraConfig(extra)
	fmt.Println("cfg", cfg)
	if cfg == nil {
		fmt.Println("config is null")
	}

	brokers := parseBrokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no Kafka brokers provided")
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("producer init error: %v", err)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil || len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"error":"invalid or empty body"}`))
			return
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: cfg.TopicURL,
			Value: sarama.ByteEncoder(body),
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"error":"kafka send failed: %v"}`, err)))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"status":"sent","topic":"%s"}`, cfg.TopicURL)))
	}), nil
}

// ---------------- Kafka helpers ----------------

func parseExtraConfig(extra map[string]interface{}) *publisherCfg {
	fmt.Println(extra)
	rawCfg, ok := extra[pluginName]
	if !ok {
		return nil
	}

	b, err := json.Marshal(rawCfg)
	if err != nil {
		return nil
	}

	cfg := &publisherCfg{}

	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil
	}

	return cfg
}

func init() {
	fmt.Println("PLUGIN INIT: pubsub-publisher loaded")
}

func parseBrokers() []string {
	env := os.Getenv("KAFKA_BROKERS")
	if env == "" {
		return nil
	}

	// split by comma and trim spaces
	parts := strings.Split(env, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			brokers = append(brokers, s)
		}
	}

	return brokers
}
