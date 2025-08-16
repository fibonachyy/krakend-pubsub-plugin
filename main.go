package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

const (
	publisherNamespace  = "pubsub/publisher"
	subscriberNamespace = "pubsub/subscriber"
)

// Publisher config
type publisherCfg struct {
	Topic_url  string `json:"topic_url,omitempty"`
	Queue_size int    `json:"queue_size,omitempty"`
}

// Subscriber config
type subscriberCfg struct {
	Topic    string `json:"topic,omitempty"`
	Group_id string `json:"group_id,omitempty"`
}

// BackendFactory holds plugin state
type BackendFactory struct {
	ctx              context.Context
	logger           logging.Logger
	bf               proxy.BackendFactory
	producer         sarama.SyncProducer
	producerHealthy  bool
	consumerHealthy  bool
	consumer         sarama.ConsumerGroup
	producerLock     sync.RWMutex
	consumerLock     sync.RWMutex
	msgQueue         chan []byte
	producerTopic    string
	subscriberTopics []string
	groupID          string
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

	// Try subscriber
	if prxy, err := f.initSubscriber(f.ctx, remote); err == nil {
		return prxy
	}

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

// ==================== Subscriber ====================
func (f *BackendFactory) initSubscriber(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	cfg := &subscriberCfg{}
	if err := getConfig(remote, subscriberNamespace, cfg); err != nil {
		return proxy.NoopProxy, err
	}

	brokers := parseBrokers(remote)
	if len(brokers) == 0 {
		return proxy.NoopProxy, fmt.Errorf("KAFKA_BROKERS not set")
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = false
	saramaCfg.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumerGroup(brokers, cfg.Group_id, saramaCfg)
	if err != nil {
		return proxy.NoopProxy, err
	}

	f.consumerLock.Lock()
	f.consumer = consumer
	f.consumerHealthy = true
	f.consumerLock.Unlock()
	f.subscriberTopics = []string{cfg.Topic}
	f.groupID = cfg.Group_id

	ef := proxy.NewEntityFormatter(remote)

	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		var msg map[string]interface{}

		handler := &singleMessageHandler{
			msg:  &msg,
			done: make(chan struct{}),
		}

		go func() {
			for {
				err := f.consumer.Consume(ctx, f.subscriberTopics, handler)
				if err != nil {
					log.Println("[Subscriber] Consume error:", err)
					f.setConsumerHealthy(false)
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
		}()

		select {
		case <-handler.done:
			f.setConsumerHealthy(true)
			resp := ef.Format(proxy.Response{Data: msg, IsComplete: true})
			return &resp, nil
		case <-time.After(5 * time.Second):
			return nil, fmt.Errorf("timeout: no message received")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, nil
}

// ==================== Helper functions ====================
func (f *BackendFactory) setProducerHealthy(val bool) {
	f.producerLock.Lock()
	f.producerHealthy = val
	f.producerLock.Unlock()
}

func (f *BackendFactory) setConsumerHealthy(val bool) {
	f.consumerLock.Lock()
	f.consumerHealthy = val
	f.consumerLock.Unlock()
}

type singleMessageHandler struct {
	msg  *map[string]interface{}
	done chan struct{}
}

func (h *singleMessageHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *singleMessageHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *singleMessageHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var m map[string]interface{}
		if err := json.Unmarshal(message.Value, &m); err != nil {
			log.Println("[Subscriber] decode error:", err)
			continue
		}
		*h.msg = m
		sess.MarkMessage(message, "")
		close(h.done)
		return nil
	}
	return nil
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

func parseBrokers(remote *config.Backend) []string {
	brokers := []string{}
	if env := remote.ExtraConfig["KAFKA_BROKERS"]; env != nil {
		if list, ok := env.([]interface{}); ok {
			for _, v := range list {
				if str, ok := v.(string); ok {
					brokers = append(brokers, str)
				}
			}
		}
	}
	return brokers
}
