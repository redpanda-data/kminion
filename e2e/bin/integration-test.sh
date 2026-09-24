#!/usr/bin/env bash
set -euo pipefail

# KMinion E2E Integration Test Script
# This script validates KMinion's end-to-end monitoring and built-in metrics
# Can be run locally or in CI

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-30}"

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

# Fetch metrics from KMinion
fetch_metrics() {
    local url="$1"
    if ! curl -sf "$url" 2>/dev/null; then
        log_error "Failed to fetch metrics from $url"
        return 1
    fi
}

# Validate E2E metrics
validate_e2e_metrics() {
    log_info "Validating E2E metrics..."

    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    # Required E2E metrics
    local required_metrics=(
        "kminion_end_to_end_messages_produced_total"
        "kminion_end_to_end_messages_received_total"
        "kminion_end_to_end_produce_latency_seconds"
        "kminion_end_to_end_roundtrip_latency_seconds"
        "kminion_end_to_end_offset_commit_latency_seconds"
    )

    local missing_metrics=()
    for metric in "${required_metrics[@]}"; do
        if ! echo "$metrics" | grep -q "^${metric}"; then
            missing_metrics+=("$metric")
        fi
    done

    if [ ${#missing_metrics[@]} -ne 0 ]; then
        log_error "Missing required E2E metrics:"
        printf '  - %s\n' "${missing_metrics[@]}"
        echo ""
        log_info "Available E2E metrics:"
        echo "$metrics" | grep -E "kminion_end_to_end_" || echo "  (none found)"
        return 1
    fi

    # Verify messages were produced and received
    local produced received
    produced=$(echo "$metrics" | grep "^kminion_end_to_end_messages_produced_total" | awk '{print $2}')
    received=$(echo "$metrics" | grep "^kminion_end_to_end_messages_received_total" | awk '{print $2}')

    if [ -z "$produced" ] || [ "$produced" = "0" ]; then
        log_error "No messages were produced (expected > 0)"
        return 1
    fi

    if [ -z "$received" ] || [ "$received" = "0" ]; then
        log_error "No messages were received (expected > 0)"
        return 1
    fi

    log_info "✅ E2E metrics validation passed"
    log_info "   Messages produced: $produced"
    log_info "   Messages received: $received"
    return 0
}

# Validate built-in KMinion metrics
validate_builtin_metrics() {
    log_info "Validating built-in KMinion metrics..."

    local metrics
    metrics=$(fetch_metrics "$METRICS_URL") || return 1

    # Core exporter metrics
    local core_metrics=(
        "kminion_exporter_up"
        "kminion_exporter_offset_consumer_records_consumed_total"
    )

    # Kafka cluster metrics
    local kafka_metrics=(
        "kminion_kafka_cluster_info"
        "kminion_kafka_broker_info"
    )

    # Topic metrics
    local topic_metrics=(
        "kminion_kafka_topic_info"
        "kminion_kafka_topic_partition_high_water_mark"
        "kminion_kafka_topic_high_water_mark_sum"
        "kminion_kafka_topic_partition_low_water_mark"
        "kminion_kafka_topic_low_water_mark_sum"
    )

    # Consumer group metrics
    local consumer_group_metrics=(
        "kminion_kafka_consumer_group_info"
        "kminion_kafka_consumer_group_members"
        "kminion_kafka_consumer_group_topic_lag"
    )

    # Log dir metrics
    local logdir_metrics=(
        "kminion_kafka_broker_log_dir_size_total_bytes"
        "kminion_kafka_topic_log_dir_size_total_bytes"
    )

    local missing_metrics=()

    # Check all metric categories
    for metric in "${core_metrics[@]}" "${kafka_metrics[@]}" "${topic_metrics[@]}" "${consumer_group_metrics[@]}" "${logdir_metrics[@]}"; do
        if ! echo "$metrics" | grep -q "^${metric}"; then
            missing_metrics+=("$metric")
        fi
    done

    if [ ${#missing_metrics[@]} -ne 0 ]; then
        log_error "Missing required built-in metrics:"
        printf '  - %s\n' "${missing_metrics[@]}"
        echo ""
        log_info "Available metrics:"
        echo "$metrics" | grep "^kminion_"
        return 1
    fi

    # Validate specific metric values
    local exporter_up
    exporter_up=$(echo "$metrics" | grep "^kminion_exporter_up" | awk '{print $2}')
    if [ "$exporter_up" != "1" ]; then
        log_error "kminion_exporter_up should be 1, got: $exporter_up"
        return 1
    fi

    # Check that cluster info has broker_count label
    if ! echo "$metrics" | grep "kminion_kafka_cluster_info" | grep -q "broker_count"; then
        log_error "kminion_kafka_cluster_info missing broker_count label"
        return 1
    fi

    # Check that we have broker info for at least one broker
    local broker_count
    broker_count=$(echo "$metrics" | grep "^kminion_kafka_broker_info" | wc -l | tr -d ' ')
    if [ "$broker_count" -lt 1 ]; then
        log_error "No broker info metrics found"
        return 1
    fi

    log_info "✅ Built-in metrics validation passed"
    log_info "   Exporter up: $exporter_up"
    log_info "   Brokers detected: $broker_count"
    return 0
}

# Wait for KMinion to be ready
wait_for_kminion() {
    log_info "Waiting for KMinion to be ready (timeout: ${WAIT_TIMEOUT}s)..."

    local attempt=0
    while [ $attempt -lt "$WAIT_TIMEOUT" ]; do
        if curl -sf "$METRICS_URL" > /dev/null 2>&1; then
            log_info "KMinion is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    log_error "KMinion did not become ready within ${WAIT_TIMEOUT}s"
    return 1
}

# Main test execution
main() {
    log_info "Starting KMinion integration tests"
    log_info "Metrics URL: $METRICS_URL"

    # Wait for KMinion to be ready
    if ! wait_for_kminion; then
        exit 1
    fi

    # Give E2E tests time to produce metrics
    log_info "Waiting ${WAIT_TIMEOUT}s for E2E tests to produce metrics..."
    sleep "$WAIT_TIMEOUT"

    local failed=0

    # Run E2E metrics validation
    if ! validate_e2e_metrics; then
        failed=1
    fi

    # Run built-in metrics validation
    if ! validate_builtin_metrics; then
        failed=1
    fi

    if [ $failed -eq 0 ]; then
        log_info "🎉 All integration tests passed!"
        return 0
    else
        log_error "❌ Some integration tests failed"
        return 1
    fi
}

# Run main if executed directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi

