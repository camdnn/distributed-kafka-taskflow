#!/usr/bin/env bash
# creates topics for kafka

# -e exists on command failure
# -u: errors on inset variables
# -o pipefail: males a fialing commanf inside a pipe fail the wh9ole line
set -euo pipefail

BOOTSTRAP_SERVER="localhost:9092"

create_topic() {
  local topic=$1
  local partitions=$2
  echo "Creating topic: $topic ($partitions partitions)"
  docker exec ftq-kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
}

create_topic "tasks" 3
create_topic "retries" 1
create_topic "dead-letter" 1
create_topic "results" 1
create_topic "task-status" 1
create_topic "alerts" 1

echo ""
echo "All topics:"
docker exec ftq-kafka kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER"
