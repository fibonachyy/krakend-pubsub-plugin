package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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

// Actual HTTP handler for KrakenD backend
func (r registerer) registerClients(_ context.Context, extra map[string]interface{}) (http.Handler, error) {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil || len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"error":"invalid or empty body"}`))
			return
		}
		
		cfg := parseExtraConfig(extra)
		fmt.Println("cfg", cfg)
		if cfg == nil || len(cfg.Brokers) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"error":"kafka config invalid"}`))
			return
		}

		producer, err := newKafkaProducer(cfg.Brokers)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf(`{"error":"producer init failed: %v"}`, err)))
			return
		}
		defer producer.Close()

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: cfg.Topic,
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
		w.Write([]byte(fmt.Sprintf(`{"status":"sent","topic":"%s"}`, cfg.Topic)))
	}), nil
}

// ---------------- Kafka helpers ----------------

type pluginCfg struct {
	Topic   string
	Brokers []string
}

func parseExtraConfig(extra map[string]interface{}) *pluginCfg {
	fmt.Println(extra)
	// rawOuter, ok := extra[pluginNamespace]
	// if !ok {
	// 	return nil
	// }

	// outer, ok := rawOuter.(map[string]interface{})
	// if !ok {
	// 	return nil
	// }

	rawCfg, ok := extra[pluginName]
	if !ok {
		return nil
	}

	b, err := json.Marshal(rawCfg)
	if err != nil {
		return nil
	}

	cfg := struct {
		Topic   string   `json:"topic_url"`
		Brokers []string `json:"brokers"`
	}{}

	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil
	}

	return &pluginCfg{
		Topic:   cfg.Topic,
		Brokers: cfg.Brokers,
	}
}

func newKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Return.Successes = true

	return sarama.NewSyncProducer(brokers, saramaCfg)
}

func init() {
	fmt.Println("PLUGIN INIT: pubsub-publisher loaded")
}