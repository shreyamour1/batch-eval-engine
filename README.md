# Batch Evaluation Engine

An asynchronous batch evaluation engine. It accepts a file of prompts, publishes
them onto Kafka, and (in later steps) fans them out across a consumer group that
calls a live inference endpoint, handles rate limits, isolates failures, and
assembles the results.

> **Status — step 1 of the build.** This slice implements `POST /jobs`: it
> accepts the batch file, stream-publishes each prompt to Kafka, and returns a
> Job ID immediately. The consumer/worker side, status/download endpoints, Redis
> state, and Resilience4j backoff are added in later steps.

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design and diagrams. In
brief: `POST /jobs` → stream-read the file → publish one message per prompt to
the Kafka topic `prompt-jobs` → return a Job ID. Publishing runs on a background
thread, so submission returns instantly even for large files.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker (for local Kafka, and for the integration test)

## Quickstart

### 1. Start Kafka

```bash
docker compose up -d
```

This runs a single-node Kafka on `localhost:9092`.

### 2. Generate a 1,000-prompt input file

```bash
python3 scripts/generate_sample_batch.py 1000 > sample_batch.json
```

A tiny 3-item `sample_batch.json` is already included if you just want to smoke-test.
Each item has the shape `{"id": "...", "prompt": "..."}`.

### 3. Build and run the app

```bash
mvn spring-boot:run
```

The service starts on `http://localhost:8080`.

### 4. Submit the batch

```bash
curl -i -F "file=@sample_batch.json" http://localhost:8080/jobs
```

Expected response (returns immediately):

```
HTTP/1.1 202 Accepted
{"jobId":"<uuid>","status":"ACCEPTED"}
```

### 5. (Optional) Verify the messages landed on Kafka

```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prompt-jobs --from-beginning --max-messages 5
```

You should see one JSON message per prompt.

## Running the tests

```bash
mvn test
```

- `PromptPublisherTest` — unit test, mocks Kafka and asserts one message per prompt.
- `JobControllerIntegrationTest` — spins up a real Kafka via Testcontainers,
  POSTs a file through the web layer, and consumes the topic to confirm the
  prompts were published. **Requires Docker to be running.**

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a batch file (multipart field `file`); returns `202` + Job ID |
| `GET` | `/job/{id}/status` | _(coming in a later step)_ |
| `GET` | `/job/{id}/download` | _(coming in a later step)_ |

## Project layout

```
src/main/java/com/example/batcheval
├── BatchEvalApplication.java        # entry point
├── api/                             # REST layer
│   ├── JobController.java
│   └── dto/JobSubmissionResponse.java
├── batch/                           # ingestion + publishing
│   ├── BatchSubmissionService.java  # request-thread orchestration
│   ├── BackgroundPublisher.java     # @Async publishing
│   ├── PromptPublisher.java         # streaming read -> Kafka
│   └── model/{Prompt,PromptTask}.java
├── job/                             # job state (in-memory for now)
│   ├── JobRegistry.java
│   ├── JobState.java
│   └── JobStatus.java
└── config/                          # Kafka + async configuration
    ├── KafkaProducerConfig.java
    ├── KafkaTopicConfig.java
    └── AsyncConfig.java
```

## Notes

- **Why a temp file?** The uploaded `MultipartFile` is only valid during the
  request. Publishing runs afterwards on a background thread, so the upload is
  copied to a temp file first, then streamed and deleted.
- **Memory:** prompts are read one at a time with Jackson's `MappingIterator`,
  so the full array is never held in memory — the same code path handles 1,000
  or 500,000 items.
