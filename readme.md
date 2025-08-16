# Krakend Kafka Publisher Plugin

A small Krakend/Lura backend plugin that publishes incoming HTTP request bodies to a Kafka topic using IBM's `sarama` SyncProducer.

## What this plugin does

* Reads plugin-specific configuration from the backend `extra_config` under the namespace `pubsub/publisher`.
* Reads Kafka broker addresses from `extra_config.KAFKA_BROKERS`.
* Creates a `sarama.SyncProducer` and publishes request bodies to the configured topic.
* Returns controlled HTTP JSON responses to the client (success or error) using Lura `proxy.Response` with `IsComplete=true` and `Metadata` (headers + status code).

## Features

* Synchronous Kafka publish (blocking until the broker acknowledges).
* Input validation (empty body, read errors).
* Health flag (`producerHealthy`) updated on send success/failure.
* Robust broker parsing from `extra_config` (accepts `[]interface{}`, `[]string`, or single string).

---

## Requirements

* Go 1.20+ (or a modern Go toolchain)
* `github.com/IBM/sarama` (used in this code)
* `github.com/luraproject/lura/v2` (Lura / Krakend types)

---

## Build (as a Krakend plugin)

Compile the plugin as a Go plugin (shared object) which Krakend can load. Example:

```bash
go build -buildmode=plugin -o krakend-kafka-publisher.so main.go
```

Place the resulting `.so` where your Krakend process can load it (or in the same directory invoked by Krakend's `plugin` configuration).

> Note: some environments (cross-compilation, different Go versions) make `.so` plugins tricky. If you run into problems, consider building on the same OS/architecture as your Krakend runtime.

---

## Configuration example (krakend `backend` entry)

Below is a minimal example showing how to wire the plugin for a `publisher` backend. The plugin reads `pubsub/publisher` config and `KAFKA_BROKERS` from `extra_config`.

```json
{
  "endpoint": "/kafka/publish",
  "method": "POST",
  "backend": [
    {
      "host": ["kafka://"],
      "disable_host_sanitize": true,
      "extra_config": {
        "pubsub/publisher": {
          "topic_url": "test-topic",
          "queue_size": 10
        },
        "KAFKA_BROKERS": ["kafka:9092"]
      }
    }
  ]
}
```

**Fields**

* `pubsub/publisher.topic_url` — Kafka topic to publish to.
* `pubsub/publisher.queue_size` — unused in the provided code but available for future buffering.
* `KAFKA_BROKERS` — broker list. Can be an array of strings, an array of interface values (JSON decoded), or a single string.

---

## How it behaves (request lifecycle)

1. The plugin is initialized at startup. If configuration or brokers are missing, plugin init returns an error and Krakend will fall back to the default backend (or fail plugin load depending on your setup).
2. On each request, the plugin reads the request body (the whole payload) and attempts to publish it to the configured Kafka topic.
3. On success the plugin returns a JSON response in `Data` with `status: sent` and the `topic` and HTTP 200.
4. On errors (empty body, read error, kafka send failure) the plugin returns a controlled JSON error response with an appropriate HTTP status code (400 or 500). The handler returns `(*proxy.Response, nil)` for these controlled responses — **do not return a non-nil `error` together with a response**, otherwise Lura will treat this as a plugin failure and ignore your response body.

---

## Important implementation notes

* **Response type**: this code uses `proxy.Response.Data` as a `map[string]interface{}` (the code base of Lura in your environment expects a map). Make sure your Lura version matches this shape. If your Lura version expects raw `[]byte` in `Data`, adapt accordingly.

* **Short-circuiting**: the plugin sets `IsComplete: true` on responses to short-circuit the pipeline so Krakend sends the response without merging.

* **HTTP status & headers**: the plugin sets `Metadata.Headers` and `Metadata.StatusCode` to ensure the client receives the intended status and `Content-Type: application/json` header.

* **Producer health**: `setProducerHealthy` toggles an internal flag on send success/failure. You can expose this via a health endpoint if desired.

---

## Testing

1. Start Krakend with the built plugin loaded.
2. Send a `POST` to `/kafka/publish` (or your configured endpoint) with a JSON body:

```bash
curl -i -X POST http://localhost:8080/kafka/publish -d '{"hello":"world"}' -H "Content-Type: application/json"
```

3. Expected responses:

* `200 OK` with body `{ "status": "sent", "topic": "<topic>" }` on success.
* `400 Bad Request` with `{ "error": "empty body" }` if body is empty.
* `500 Internal Server Error` on Kafka send failure with `{ "error": "kafka send failed: ..." }`.

---

## Troubleshooting

* If you see Krakend returning a generic 502/500 and your custom JSON is missing, check that your handler is returning `(*proxy.Response, nil)` (controlled response) and not `(..., err)` together with the response.
* If Kafka messages are not delivered, ensure `KAFKA_BROKERS` is reachable from the Krakend process and the topic exists (or that the broker allows topic creation).
* Check your Lura/Krakend and `sarama` versions compatibility.

---

## Future improvements

* Add async buffering and retries using the `queue_size` field.
* Add structured logging on send success/failure.
* Add graceful shutdown to close the `sarama.SyncProducer`.
* Add a subscriber counterpart (already discussed in other code) that consumes messages via a consumer group.

---

## License

MIT - adapt as needed.
