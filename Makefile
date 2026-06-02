# Makefile — fault-tolerant task queue on Kafka
COMPOSE := docker compose -f deployments/docker-compose.yml
BOOTSTRAP := localhost:9092

.PHONY: up down topics worker ingest retry monitor logs ps clean \
        watch-status watch-results watch-alerts watch-dlq inject-emergency inject-bad

## --- Infrastructure ---

# Bring up Kafka and create topics (idempotent — safe to re-run)
up:
	$(COMPOSE) up -d
	@echo "waiting for broker..."
	@sleep 5
	./scripts/create-topics.sh
	@echo "ready."

# Tear everything down
down:
	$(COMPOSE) down

# Create/verify topics only
topics:
	./scripts/create-topics.sh

# Show running containers
ps:
	$(COMPOSE) ps

## --- Application processes (each runs in the foreground; use separate terminals) ---

# Run ONE worker. For 3 workers, run `make worker` in 3 terminals.
worker:
	go run ./cmd/worker

# Run the ingester (live ADS-B -> tasks)
producer:
	go run ./cmd/producer

# Run the retry handler
retry:
	go run ./cmd/retry-handler

# Run the monitor
monitor:
	go run ./cmd/monitor

## --- Observability: tail topics ---

watch-status:
	$(COMPOSE) exec kafka kafka-console-consumer --topic task-status --bootstrap-server $(BOOTSTRAP) --from-beginning

watch-results:
	$(COMPOSE) exec kafka kafka-console-consumer --topic results --bootstrap-server $(BOOTSTRAP) --from-beginning

watch-alerts:
	$(COMPOSE) exec kafka kafka-console-consumer --topic alerts --bootstrap-server $(BOOTSTRAP) --from-beginning

watch-dlq:
	$(COMPOSE) exec kafka kafka-console-consumer --topic dead-letter --bootstrap-server $(BOOTSTRAP) --from-beginning

## --- Demo helpers: inject test tasks ---

# Inject an emergency (7700) task to prove the alert path
inject-emergency:
	@echo '{"id":"emergency-test","type":"process_aircraft","payload":{"hex":"test99","flight":"TEST911","lat":33.9,"lon":-118.4,"squawk":"7700","observed_at":"2026-06-01T13:00:00Z"},"attempts":0,"created_at":"2026-06-01T13:00:00Z"}' | \
	$(COMPOSE) exec -T kafka kafka-console-producer --topic tasks --bootstrap-server $(BOOTSTRAP)

# Inject a bad-data task to prove the retry -> DLQ path
inject-bad:
	@echo '{"id":"retry-test","type":"process_aircraft","payload":{"hex":"badplane","lat":200.5,"lon":-118.4,"observed_at":"2026-06-01T13:00:00Z"},"attempts":0,"created_at":"2026-06-01T13:00:00Z"}' | \
	$(COMPOSE) exec -T kafka kafka-console-producer --topic tasks --bootstrap-server $(BOOTSTRAP)
