# KMinion E2E Integration Tests

This directory contains end-to-end integration tests for KMinion, including scripts to set up a test Kafka cluster and validate KMinion's metrics.

## Overview

The E2E test suite validates:
- **End-to-End Monitoring**: KMinion's ability to produce, consume, and measure message latencies
- **Built-in Metrics**: Core exporter metrics, Kafka cluster info, broker info, and topic metrics

## Scripts

All scripts are located in the `e2e/bin/` directory.

### `setup-kafka.sh`
Manages a Kafka 4.0 cluster with KRaft mode in Docker for testing.

**Usage:**
```bash
# Start Kafka
./e2e/bin/setup-kafka.sh start

# Stop Kafka
./e2e/bin/setup-kafka.sh stop

# Restart Kafka
./e2e/bin/setup-kafka.sh restart

# Check status
./e2e/bin/setup-kafka.sh status

# View logs
./e2e/bin/setup-kafka.sh logs
```

**Environment Variables:**
- `KAFKA_VERSION`: Kafka version (default: `4.1.0`)
- `KAFKA_PORT`: Kafka port (default: `9092`)
- `CONTAINER_NAME`: Docker container name (default: `broker`)
- `WAIT_TIMEOUT`: Wait timeout in seconds (default: `60`)

**Example:**
```bash
# Start Kafka on a different port
KAFKA_PORT=9093 ./e2e/bin/setup-kafka.sh start
```

### `start-kminion.sh`
Starts KMinion with E2E configuration and waits for it to be ready.

**Usage:**
```bash
# Start KMinion
./e2e/bin/start-kminion.sh start

# Stop KMinion
./e2e/bin/start-kminion.sh stop

# Restart KMinion
./e2e/bin/start-kminion.sh restart

# Check status
./e2e/bin/start-kminion.sh status

# View logs
./e2e/bin/start-kminion.sh logs
```

**Environment Variables:**
- `CONFIG_FILE`: Path to config file (default: `e2e/bin/test-config.yaml`)
- `KMINION_BIN`: Path to KMinion binary (default: `./kminion`)
- `LOG_FILE`: Path to log file (default: `kminion.log`)
- `METRICS_URL`: Metrics endpoint URL (default: `http://localhost:8080/metrics`)
- `WAIT_TIMEOUT`: Wait timeout in seconds (default: `30`)

**Example:**
```bash
# Start with custom config
CONFIG_FILE=my-config.yaml ./e2e/bin/start-kminion.sh start
```

### `integration-test.sh`
Validates KMinion's E2E and built-in metrics.

**Usage:**
```bash
# Run all integration tests
./e2e/bin/integration-test.sh
```

**Environment Variables:**
- `METRICS_URL`: KMinion metrics endpoint (default: `http://localhost:8080/metrics`)
- `WAIT_TIMEOUT`: Wait timeout in seconds (default: `30`)

**Example:**
```bash
# Test against a different KMinion instance
METRICS_URL=http://localhost:8081/metrics ./e2e/bin/integration-test.sh
```

## Running Tests Locally

### Prerequisites
- Docker
- `curl`
- `nc` (netcat)
- Go 1.21+ (for building KMinion)
- GNU Make

### Quick Start with Makefile

The easiest way to run E2E tests is using the Makefile targets:

```bash
# Run the complete E2E test suite (setup, build, test, cleanup)
make e2e-full
```

This single command will:
1. Start Kafka cluster
2. Build KMinion
3. Start KMinion with E2E config
4. Run integration tests
5. Cleanup everything

### Step-by-Step with Makefile

If you prefer to run steps individually:

```bash
# 1. Start Kafka cluster
make e2e-setup

# 2. Build KMinion
make build

# 3. Start KMinion with E2E config
make e2e-start

# 4. Run integration tests
make e2e-test

# 5. Cleanup
make e2e-cleanup
```

### Manual Test Flow (without Makefile)

1. **Start Kafka cluster:**
   ```bash
   ./e2e/bin/setup-kafka.sh start
   ```

2. **Build KMinion:**
   ```bash
   go build -o kminion .
   ```

3. **Start KMinion:**
   ```bash
   ./e2e/bin/start-kminion.sh start
   ```

4. **Run integration tests:**
   ```bash
   ./e2e/bin/integration-test.sh
   ```

5. **Cleanup:**
   ```bash
   ./e2e/bin/start-kminion.sh stop
   ./e2e/bin/setup-kafka.sh stop
   ```

### Available Makefile Targets

```bash
make help           # Show all available targets
make build          # Build KMinion binary
make test           # Run unit tests
make e2e-setup      # Start Kafka cluster for E2E testing
make e2e-start      # Start KMinion with E2E configuration
make e2e-stop       # Stop KMinion
make e2e-test       # Run E2E integration tests (requires Kafka and KMinion)
make e2e-cleanup    # Stop and cleanup Kafka cluster and KMinion
make e2e-full       # Run full E2E test suite (setup, build, start, test, cleanup)
```

## CI Integration

The GitHub Actions workflow (`.github/workflows/e2e-tests.yaml`) uses Makefile targets:

```yaml
- name: Start Kafka 4 with KRaft
  run: make e2e-setup

- name: Build KMinion
  run: make build

- name: Start KMinion with E2E tests
  run: make e2e-start

- name: Run integration tests
  run: make e2e-test

- name: Cleanup
  run: make e2e-cleanup
```

## Validated Metrics

### E2E Metrics
- `kminion_end_to_end_messages_produced_total` - Total messages produced
- `kminion_end_to_end_messages_received_total` - Total messages received
- `kminion_end_to_end_produce_latency_seconds` - Producer ack latency
- `kminion_end_to_end_roundtrip_latency_seconds` - End-to-end roundtrip latency
- `kminion_end_to_end_offset_commit_latency_seconds` - Offset commit latency

### Built-in Metrics
- `kminion_exporter_up` - Exporter health status
- `kminion_exporter_offset_consumer_records_consumed_total` - Offset consumer records
- `kminion_kafka_cluster_info` - Cluster metadata
- `kminion_kafka_broker_info` - Broker information
- `kminion_kafka_topic_info` - Topic metadata
- `kminion_kafka_topic_partition_count` - Partition counts
- `kminion_kafka_topic_partition_high_water_mark` - Partition offsets
- `kminion_kafka_topic_high_water_mark_sum` - Aggregated topic offsets
- `kminion_kafka_consumer_group_info` - Consumer group information
- `kminion_kafka_broker_log_dir_size_total_bytes` - Broker log directory sizes

## Troubleshooting

### Kafka won't start
```bash
# Check logs
./e2e/bin/setup-kafka.sh logs

# Restart
./e2e/bin/setup-kafka.sh restart
```

### KMinion not producing metrics
```bash
# Check if KMinion is running
curl http://localhost:8080/metrics

# Check KMinion logs
tail -f kminion.log
```

### Port conflicts
```bash
# Use different port
KAFKA_PORT=9093 ./e2e/bin/setup-kafka.sh start

# Update KMinion config to use localhost:9093
```

## Development

To add new test validations, edit `integration-test.sh` and add new validation functions following the existing pattern:

```bash
validate_my_new_metric() {
    log_info "Validating my new metric..."
    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    # Your validation logic here

    log_info "✅ My new metric validation passed"
    return 0
}
```

Then call it from the `main()` function.

