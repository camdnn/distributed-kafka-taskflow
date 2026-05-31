# kafka-taskflow

A fault-tolerant distributed task queue built with Go and Apache Kafka.

kafka-taskflow demonstrates durable asynchronous job processing with Kafka topics, consumer groups, worker pools, retries with exponential backoff, dead-letter queues, task status streams, and a live CLI monitor.

fault-tolerant-task-queue/
├── cmd/
│   ├── submitter/main.go
│   ├── worker/main.go
│   ├── retry-handler/main.go
│   └── monitor/main.go
├── internal/
│   ├── task/             # Task domain model
│   ├── queue/            # Kafka producer/consumer wrappers
│   ├── worker/           # Worker pool, executor, idempotency
│   ├── retry/            # Retry handler, backoff
│   ├── monitor/          # Dashboard logic
│   └── config/           # Config loader
├── scripts/
│   └── create-topics.sh  # creats topics for kafka
├── deployments/
│   └── docker-compose.yml
├── configs/
│   └── config.yaml
├── go.mod
├── Makefile
└── README.md


passing payload using json.RawMessage


naming convention:
JSON: snake_case
