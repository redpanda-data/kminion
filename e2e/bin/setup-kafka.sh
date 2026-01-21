#!/usr/bin/env bash
set -euo pipefail

# Kafka Setup Script for E2E Testing
# Starts a Kafka 4.0 cluster with KRaft mode in Docker

KAFKA_VERSION="${KAFKA_VERSION:-4.1.0}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
CONTAINER_NAME="${CONTAINER_NAME:-broker}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Check if Kafka container is already running
check_existing_container() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_warn "Container '$CONTAINER_NAME' already exists"
        log_info "Removing existing container..."
        docker rm -f "$CONTAINER_NAME" > /dev/null 2>&1 || true
    fi
}

# Start Kafka container
start_kafka() {
    log_info "Starting Kafka $KAFKA_VERSION with KRaft mode..."

    docker run -d \
        --name "$CONTAINER_NAME" \
        -p "${KAFKA_PORT}:9092" \
        -p 9093:9093 \
        -e KAFKA_NODE_ID=1 \
        -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:${KAFKA_PORT} \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        -e KAFKA_NUM_PARTITIONS=3 \
        "apache/kafka:${KAFKA_VERSION}"

    log_info "Kafka container started: $CONTAINER_NAME"
}

# Wait for Kafka to be ready
wait_for_kafka() {
    log_info "Waiting for Kafka to be ready (timeout: ${WAIT_TIMEOUT}s)..."

    local attempt=0
    local ready=false

    while [ $attempt -lt "$WAIT_TIMEOUT" ]; do
        # Try to get broker API versions directly
        if docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server "localhost:${KAFKA_PORT}" > /dev/null 2>&1; then
            log_info "✅ Kafka is ready and responding to requests!"
            ready=true
            break
        fi

        attempt=$((attempt + 1))
        if [ $((attempt % 10)) -eq 0 ]; then
            log_info "Attempt $attempt/$WAIT_TIMEOUT: Kafka not ready yet, waiting..."
        fi
        sleep 1
    done

    if [ "$ready" = false ]; then
        log_error "Kafka failed to start within ${WAIT_TIMEOUT}s"
        log_error "Container logs:"
        docker logs "$CONTAINER_NAME" 2>&1 | tail -50
        return 1
    fi

    return 0
}

# Verify Kafka is running
verify_kafka() {
    log_info "Verifying Kafka cluster..."

    if ! docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server "localhost:${KAFKA_PORT}"; then
        log_error "Failed to verify Kafka cluster"
        return 1
    fi

    log_info "Kafka cluster is up and running!"
    return 0
}

# Stop and remove Kafka container
stop_kafka() {
    log_info "Stopping Kafka container..."

    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        docker stop "$CONTAINER_NAME" > /dev/null 2>&1 || true
        docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || true
        log_info "Kafka container stopped and removed"
    else
        log_warn "Container '$CONTAINER_NAME' not found"
    fi
}

# Show usage
usage() {
    cat <<EOF
Usage: $0 [COMMAND]

Commands:
    start       Start Kafka container (default)
    stop        Stop and remove Kafka container
    restart     Restart Kafka container
    status      Check Kafka container status
    logs        Show Kafka container logs

Environment Variables:
    KAFKA_VERSION       Kafka version (default: 4.1.0)
    KAFKA_PORT          Kafka port (default: 9092)
    CONTAINER_NAME      Container name (default: broker)
    WAIT_TIMEOUT        Wait timeout in seconds (default: 60)

Examples:
    $0 start
    KAFKA_PORT=9093 $0 start
    $0 logs
    $0 stop
EOF
}

# Main execution
main() {
    local command="${1:-start}"

    case "$command" in
        start)
            check_existing_container
            start_kafka
            wait_for_kafka
            verify_kafka
            ;;
        stop)
            stop_kafka
            ;;
        restart)
            stop_kafka
            sleep 2
            check_existing_container
            start_kafka
            wait_for_kafka
            verify_kafka
            ;;
        status)
            if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
                log_info "Kafka container is running"
                docker ps --filter "name=${CONTAINER_NAME}"
            else
                log_warn "Kafka container is not running"
                exit 1
            fi
            ;;
        logs)
            if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
                docker logs "$CONTAINER_NAME"
            else
                log_error "Container '$CONTAINER_NAME' not found"
                exit 1
            fi
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

# Run main if executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi

