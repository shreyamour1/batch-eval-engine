# Batch Evaluation Engine

An asynchronous batch evaluation engine. It accepts a file of prompts, publishes
them onto Kafka, and (in later steps) fans them out across a consumer group that
calls a live inference endpoint, handles rate limits, isolates failures, and
assembles the results.

> **Status.** The full pipeline is implemented: `POST /jobs` stream-publishes to
> Kafka, a bounded consumer group calls the inference endpoint with Resilience4j
> retry/backoff for 429 and transient 5xx, and `GET /job/{id}/status` /
> `GET /job/{id}/download` expose progress and results. Job state is in-memory
> for now (Redis and object storage are planned scale-ups).

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

### 5. Poll job status and download results

```bash
JOB_ID="<uuid from step 4>"

curl http://localhost:8080/job/$JOB_ID/status
curl http://localhost:8080/job/$JOB_ID/download
```

Status moves through `PENDING` → `PUBLISHING` → `RUNNING` → `COMPLETED`. Download
returns `409 Conflict` until the job is complete.

### 6. (Optional) Verify the messages landed on Kafka

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
- `InferenceClientTest` — WireMock proves 429 retry and non-retry on 400.
- `JobQueryControllerTest` — status and download endpoints.
- `JobControllerIntegrationTest` — Kafka publish path via Testcontainers.
- `BatchPipelineIntegrationTest` — end-to-end: submit → consume → inference → download.
  **Requires Docker to be running.**

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a batch file (multipart field `file`); returns `202` + Job ID |
| `GET` | `/job/{id}/status` | Job progress: status, total, succeeded, failed |
| `GET` | `/job/{id}/download` | Compiled results array (409 until complete) |

## Project layout

```
src/main/java/com/example/batcheval
├── BatchEvalApplication.java        # entry point
├── api/                             # REST layer
│   ├── JobController.java           # POST /jobs
│   ├── JobQueryController.java      # GET /job/{id}/status, /download
│   └── dto/
├── worker/                          # Kafka consumer + inference
│   ├── PromptWorker.java
│   ├── InferenceClient.java
│   └── ResultWriter.java
├── batch/                           # ingestion + publishing
│   ├── BatchSubmissionService.java  # request-thread orchestration
│   ├── BackgroundPublisher.java     # @Async publishing
│   ├── PromptPublisher.java         # streaming read -> Kafka
│   └── model/{Prompt,PromptTask}.java
├── job/                             # job state (in-memory for now)
│   ├── JobRegistry.java
│   ├── JobState.java
│   └── JobStatus.java
└── config/
    ├── KafkaProducerConfig.java
    ├── KafkaConsumerConfig.java
    ├── KafkaTopicConfig.java
    ├── Resilience4jConfig.java
    └── AsyncConfig.java
```

## Notes

- **Why a temp file?** The uploaded `MultipartFile` is only valid during the
  request. Publishing runs afterwards on a background thread, so the upload is
  copied to a temp file first, then streamed and deleted.
- **Memory:** prompts are read one at a time with Jackson's `MappingIterator`,
  so the full array is never held in memory — the same code path handles 1,000
  or 500,000 items.
