// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

// Plugin constants
const (
	publisherNamespace  = "github.com/devopsfaith/krakend-pubsub/publisher"
	subscriberNamespace = "github.com/devopsfaith/krakend-pubsub/subscriber"
)

// Publisher config
type publisherCfg struct {
	TopicURL  string   `json:"topic_url,omitempty"`
	QueueSize int      `json:"queue_size,omitempty"`
	Brokers   []string `json:"brokers,omitempty"`
}

// BackendFactory holds state for Kafka publisher
type BackendFactory struct {
	ctx             context.Context
	logger          logging.Logger
	bf              proxy.BackendFactory
	producer        sarama.SyncProducer
	producerHealthy bool
	producerLock    sync.RWMutex
	producerTopic   string
}

// NewBackendFactory constructor
func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) *BackendFactory {
	fmt.Println("NewBackendFactory")

	return &BackendFactory{
		ctx:    ctx,
		logger: logger,
		bf:     bf,
	}
}

// New returns a proxy.Proxy for the backend
func (f *BackendFactory) New(remote *config.Backend) proxy.Proxy {
	fmt.Println("New")
	prxy, err := f.initPublisher(f.ctx, remote)
	if err != nil {
		if f.logger != nil {
			f.logger.Warning(fmt.Sprintf("publisher not initialized: %v", err))
		}
		// fallback to NoopProxy instead of calling bf(remote)
		return proxy.NoopProxy
	}
	return prxy
}

// initPublisher initializes Kafka producer and returns a proxy
func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	fmt.Println("initPublisher")
	if remote == nil {
		return nil, fmt.Errorf("remote backend is nil")
	}

	cfg := &publisherCfg{}
	if err := getConfig(remote, pluginNamespace, cfg); err != nil {
		return nil, fmt.Errorf("config error: %v", err)
	}
	if cfg.TopicURL == "" {
		return nil, fmt.Errorf("missing topic_url in %s", pluginNamespace)
	}
	f.producerTopic = cfg.TopicURL
	f.logger.Warning("remote", remote)
	f.logger.Warning("cfg", cfg)
	brokers := cfg.Brokers
	if len(brokers) == 0 {
		brokers = parseBrokers(remote)
	}
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

	f.producerLock.Lock()
	f.producer = producer
	f.producerHealthy = true
	f.producerLock.Unlock()

	// Return the proxy
	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		fmt.Println("test")
		var body []byte
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				return proxyErrorResponse(fmt.Sprintf("read body error: %v", err), http.StatusBadRequest)
			}
			body = b
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		if len(body) == 0 {
			return proxyErrorResponse("empty body", http.StatusBadRequest)
		}

		_, _, err := f.producer.SendMessage(&sarama.ProducerMessage{
			Topic: f.producerTopic,
			Value: sarama.ByteEncoder(body),
		})
		if err != nil {
			f.setProducerHealthy(false)
			if f.logger != nil {
				f.logger.Error(fmt.Sprintf("[PLUGIN] kafka send failed: %v", err))
			}
			return proxyErrorResponse(fmt.Sprintf("kafka send failed: %v", err), http.StatusInternalServerError)
		}

		f.setProducerHealthy(true)
		return proxyOKResponse(f.producerTopic)
	}, nil
}

func (f *BackendFactory) setProducerHealthy(val bool) {
	f.producerLock.Lock()
	f.producerHealthy = val
	f.producerLock.Unlock()
}

// parseBrokers reads Kafka brokers from extra_config
func parseBrokers(remote *config.Backend) []string {
	fmt.Println(remote)
	if remote == nil || remote.ExtraConfig == nil {
		return nil
	}

	// direct KAFKA_BROKERS key
	if v, ok := remote.ExtraConfig["KAFKA_BROKERS"]; ok {
		switch vv := v.(type) {
		case []interface{}:
			out := make([]string, 0, len(vv))
			for _, e := range vv {
				if s, ok := e.(string); ok && s != "" {
					out = append(out, s)
				}
			}
			return out
		case []string:
			return vv
		case string:
			if vv != "" {
				return []string{vv}
			}
		}
	}

	// nested under plugin namespace
	if v, ok := remote.ExtraConfig[pluginNamespace]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			if b, ok := m["brokers"]; ok {
				switch bb := b.(type) {
				case []interface{}:
					out := make([]string, 0, len(bb))
					for _, e := range bb {
						if s, ok := e.(string); ok && s != "" {
							out = append(out, s)
						}
					}
					return out
				case []string:
					return bb
				case string:
					if bb != "" {
						return []string{bb}
					}
				}
			}
		}
	}

	return nil
}

// getConfig unmarshals extra_config[namespace] into v
func getConfig(remote *config.Backend, namespace string, v interface{}) error {
	fmt.Println("getConfig", remote)
	data, ok := remote.ExtraConfig[namespace]
	if !ok {
		return fmt.Errorf("%s not found in extra_config", namespace)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// helper for error response
func proxyErrorResponse(msg string, code int) (*proxy.Response, error) {
	return &proxy.Response{
		IsComplete: true,
		Data:       map[string]interface{}{"error": msg},
		Metadata: proxy.Metadata{
			StatusCode: code,
			Headers:    map[string][]string{"Content-Type": {"application/json"}},
		},
	}, nil
}

// helper for success response
func proxyOKResponse(topic string) (*proxy.Response, error) {
	return &proxy.Response{
		IsComplete: true,
		Data:       map[string]interface{}{"status": "sent", "topic": topic},
		Metadata: proxy.Metadata{
			StatusCode: http.StatusOK,
			Headers:    map[string][]string{"Content-Type": {"application/json"}},
		},
	}, nil
}

func init() {
	fmt.Println("PLUGIN INIT: pubsub-publisher loaded")
}
