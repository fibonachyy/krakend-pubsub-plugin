package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

// Namespace constants
const (
	publisherNamespace  = "github.com/devopsfaith/krakend-pubsub/publisher"
	subscriberNamespace = "github.com/devopsfaith/krakend-pubsub/subscriber"
)

// Config structs
type publisherCfg struct {
	Topic_url  string
	Queue_size int
}

type subscriberCfg struct {
	Subscription_url string
	Group_id         string
	Batch_size       int
}

// BackendFactory main struct
type BackendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
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
	if prxy, err := f.initSubscriber(f.ctx, remote); err == nil {
		return prxy
	}
	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}
	return f.bf(remote)
}

// ==================== Publisher ====================
func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	cfg := &publisherCfg{}
	if err := getConfig(remote, publisherNamespace, cfg); err != nil {
		return proxy.NoopProxy, err
	}

	brokers := []string{}
	if env := remote.ExtraConfig["KAFKA_BROKERS"]; env != nil {
		if list, ok := env.([]interface{}); ok {
			for _, v := range list {
				brokers = append(brokers, v.(string))
			}
		}
	}
	if len(brokers) == 0 {
		return proxy.NoopProxy, fmt.Errorf("KAFKA_BROKERS not set")
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, saramaCfg)
	if err != nil {
		return proxy.NoopProxy, err
	}

	if cfg.Queue_size < 1 {
		cfg.Queue_size = 1
	}
	msgQueue := make(chan []byte, cfg.Queue_size)

	// Worker goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(msgQueue)
				producer.Close()
				return
			case msg, ok := <-msgQueue:
				if !ok {
					return
				}
				for {
					_, _, err := producer.SendMessage(&sarama.ProducerMessage{
						Topic: cfg.Topic_url,
						Value: sarama.ByteEncoder(msg),
					})
					if err != nil {
						f.logger.Error("[Publisher] Kafka send failed, retrying:", err)
						time.Sleep(1 * time.Second)
						continue
					}
					break
				}
			}
		}
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		select {
		case msgQueue <- body:
			return &proxy.Response{IsComplete: true}, nil
		default:
			return nil, fmt.Errorf("queue full, cannot produce message")
		}
	}, nil
}

// ==================== Subscriber ====================
func (f *BackendFactory) initSubscriber(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	cfg := &subscriberCfg{}
	if err := getConfig(remote, subscriberNamespace, cfg); err != nil {
		return proxy.NoopProxy, err
	}

	if cfg.Batch_size < 1 {
		cfg.Batch_size = 1
	}

	brokers := []string{}
	if env := remote.ExtraConfig["KAFKA_BROKERS"]; env != nil {
		if list, ok := env.([]interface{}); ok {
			for _, v := range list {
				brokers = append(brokers, v.(string))
			}
		}
	}
	if len(brokers) == 0 {
		return proxy.NoopProxy, fmt.Errorf("KAFKA_BROKERS not set")
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Version = sarama.V2_1_0_0

	consumer, err := sarama.NewConsumerGroup(brokers, cfg.Group_id, saramaCfg)
	if err != nil {
		return proxy.NoopProxy, err
	}

	ef := proxy.NewEntityFormatter(remote)

	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		batch := []map[string]interface{}{}
		handler := &consumerGroupHandler{
			batchSize: cfg.Batch_size,
			decoder:   remote.Decoder,
			batch:     &batch,
			done:      make(chan struct{}),
		}

		go func() {
			for {
				err := consumer.Consume(ctx, []string{cfg.Subscription_url}, handler)
				if err != nil {
					log.Println("[Subscriber] Consume error:", err)
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
		}()

		<-handler.done
		resp := &proxy.Response{Data: batch, IsComplete: true}
		resp = ef.Format(*resp)
		return resp, nil
	}, nil
}

// ==================== Helper ====================
type consumerGroupHandler struct {
	batchSize int
	decoder   config.Decoder
	batch     *[]map[string]interface{}
	done      chan struct{}
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	count := 0
	for msg := range claim.Messages() {
		var data map[string]interface{}
		h.decoder(bytes.NewBuffer(msg.Value), &data)
		*h.batch = append(*h.batch, data)
		sess.MarkMessage(msg, "")
		count++
		if count >= h.batchSize {
			break
		}
	}
	close(h.done)
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
