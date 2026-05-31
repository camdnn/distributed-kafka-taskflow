# ATC Alert System with Task Queue from Realtime ADSB data using Apache Kafka

Developers: Camden Mann and Roan Morgan

## Main Components

### Two Types of Producer:

#### 1. Task Initializer
- Gets API data from [ADSB](https://api.adsb.lol/docs)
- creates a new task
- adds it to the queue

#### 2. Task Processor
- Sets the task to in-progress 
- process task
- publishes task to either completed, retry, deadletter, or alert topics  

### Two Types of Consumer:

#### 1. Task Processor Helper
- pulls tasks from queue topic for producer to analyze

#### 2. Web Dashboard
- Subscribes from the alert topic and feeds a live dashboard ATC'ers can use for quick, at-a-glance, information

### Broker/s:
- Just a normal Apache Kafka environment using [KRaft](https://developer.confluent.io/learn/kraft/)
- Holds relevant topics in persistent disk
- Fault tolerance of topic data based on raft protocol as long as a quorum 
can be reached (requires 3 KRaft controllers to survive one controller outage)

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
