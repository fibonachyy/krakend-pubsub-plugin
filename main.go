package main

import (
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

const (
	publisherNamespace = "pubsub/publisher"
)

// Publisher config
type publisherCfg struct {
	Topic_url  string `json:"topic_url,omitempty"`
	Queue_size int    `json:"queue_size,omitempty"`
}

// BackendFactory holds plugin state
type BackendFactory struct {
	ctx             context.Context
	logger          logging.Logger
	bf              proxy.BackendFactory
	producer        sarama.SyncProducer
	producerHealthy bool
	producerLock    sync.RWMutex
	producerTopic   string
}

// Constructor
func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) *BackendFactory {
	return &BackendFactory{
		ctx:    ctx,
		logger: logger,
		bf:     bf,
	}
}

// Main New method
func (f *BackendFactory) New(remote *config.Backend) proxy.Proxy {

	// Try publisher
	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}

	// fallback
	return f.bf(remote)
}

// ==================== Publisher ====================
func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	cfg := &publisherCfg{}
	if err := getConfig(remote, publisherNamespace, cfg); err != nil {
		return nil, fmt.Errorf("config error: %v", err)
	}

	brokers := parseBrokers(remote)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS not set")
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
	f.producerTopic = cfg.Topic_url

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		var body []byte
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				respBody := map[string]interface{}{"error": fmt.Sprintf("read body error: %v", err)}
				return &proxy.Response{
					IsComplete: true,
					Data:       respBody,
					Metadata: proxy.Metadata{
						Headers:    map[string][]string{"Content-Type": {"application/json"}},
						StatusCode: http.StatusBadRequest,
					},
				}, nil
			}
			body = b
		}

		if len(body) == 0 {
			respBody := map[string]interface{}{"error": "empty body"}
			return &proxy.Response{
				IsComplete: true,
				Data:       respBody,
				Metadata: proxy.Metadata{
					Headers:    map[string][]string{"Content-Type": {"application/json"}},
					StatusCode: http.StatusBadRequest,
				},
			}, nil
		}

		_, _, err := f.producer.SendMessage(&sarama.ProducerMessage{
			Topic: f.producerTopic,
			Value: sarama.ByteEncoder(body),
		})
		if err != nil {
			f.setProducerHealthy(false)
			respBody := map[string]interface{}{"error": fmt.Sprintf("kafka send failed: %v", err)}
			return &proxy.Response{
				IsComplete: true,
				Data:       respBody,
				Metadata: proxy.Metadata{
					Headers:    map[string][]string{"Content-Type": {"application/json"}},
					StatusCode: http.StatusInternalServerError,
				},
			}, nil
		}

		f.setProducerHealthy(true)
		okBody := map[string]interface{}{
			"status": "sent",
			"topic":  f.producerTopic,
		}
		return &proxy.Response{
			IsComplete: true,
			Data:       okBody,
			Metadata: proxy.Metadata{
				Headers:    map[string][]string{"Content-Type": {"application/json"}},
				StatusCode: http.StatusOK,
			},
		}, nil
	}, nil
}

// helper: read brokers from extra_config (handles []interface{} from JSON)
func parseBrokers(remote *config.Backend) []string {
	if remote == nil || remote.ExtraConfig == nil {
		return nil
	}
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
	return nil
}

// ==================== Helper functions ====================
func (f *BackendFactory) setProducerHealthy(val bool) {
	f.producerLock.Lock()
	f.producerHealthy = val
	f.producerLock.Unlock()
}

func getConfig(remote *config.Backend, namespace string, v interface{}) error {
	data, ok := remote.ExtraConfig[namespace]
	if !ok {
		return fmt.Errorf("%s not found in extra_config", namespace)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &v)
}
