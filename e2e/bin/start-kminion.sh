#!/usr/bin/env bash
set -euo pipefail

# KMinion Startup Script for E2E Testing
# Starts KMinion with E2E configuration and waits for it to be ready

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/test-config.yaml}"
KMINION_BIN="${KMINION_BIN:-./kminion}"
LOG_FILE="${LOG_FILE:-kminion.log}"
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

# Check if KMinion binary exists
check_binary() {
    if [ ! -f "$KMINION_BIN" ]; then
        log_error "KMinion binary not found at: $KMINION_BIN"
        log_info "Run 'make build' first"
        return 1
    fi
    log_info "Found KMinion binary: $KMINION_BIN"
}

# Check if config file exists
check_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        log_error "Config file not found at: $CONFIG_FILE"
        return 1
    fi
    log_info "Using config file: $CONFIG_FILE"
}

# Start KMinion in background
start_kminion() {
    log_info "Starting KMinion..."

    # Start KMinion in background and redirect output to log file
    # KMinion uses CONFIG_FILEPATH environment variable
    CONFIG_FILEPATH="$CONFIG_FILE" "$KMINION_BIN" > "$LOG_FILE" 2>&1 &
    local pid=$!

    # Save PID to file for later cleanup
    echo "$pid" > kminion.pid

    log_info "KMinion started with PID: $pid"
    log_info "Logs: $LOG_FILE"

    echo "$pid"
}

# Wait for KMinion to be ready
wait_for_kminion() {
    local pid=$1
    log_info "Waiting for KMinion to be ready (timeout: ${WAIT_TIMEOUT}s)..."

    # Give KMinion a few seconds to initialize before checking
    sleep 3

    local attempt=0
    while [ $attempt -lt "$WAIT_TIMEOUT" ]; do
        # Check if metrics endpoint is responding
        if curl -sf "$METRICS_URL" > /dev/null 2>&1; then
            log_info "✅ KMinion is ready!"
            return 0
        fi

        # Check if process died (only after initial startup period)
        if [ $attempt -gt 5 ]; then
            if ! kill -0 "$pid" 2>/dev/null; then
                log_error "KMinion process died unexpectedly"
                log_error "Last 30 lines of log:"
                tail -30 "$LOG_FILE" 2>&1 || true
                return 1
            fi
        fi

        attempt=$((attempt + 1))
        if [ $((attempt % 5)) -eq 0 ]; then
            log_info "Attempt $attempt/$WAIT_TIMEOUT: KMinion not ready yet, waiting..."
        fi
        sleep 1
    done

    log_error "KMinion failed to start within ${WAIT_TIMEOUT}s"
    log_error "Last 50 lines of log:"
    tail -50 "$LOG_FILE" 2>&1 || true
    return 1
}

# Stop KMinion
stop_kminion() {
    if [ -f kminion.pid ]; then
        local pid
        pid=$(cat kminion.pid)
        log_info "Stopping KMinion (PID: $pid)..."

        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 2

            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                log_warn "Force killing KMinion..."
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi

        rm -f kminion.pid
        log_info "KMinion stopped"
    else
        log_warn "No PID file found"
    fi
}

# Show usage
usage() {
    cat <<EOF
Usage: $0 [COMMAND]

Commands:
    start       Start KMinion and wait for it to be ready (default)
    stop        Stop KMinion
    restart     Restart KMinion
    status      Check if KMinion is running
    logs        Show KMinion logs

Environment Variables:
    CONFIG_FILE     Path to config file (default: e2e/bin/test-config.yaml)
    KMINION_BIN     Path to KMinion binary (default: ./kminion)
    LOG_FILE        Path to log file (default: kminion.log)
    METRICS_URL     Metrics endpoint URL (default: http://localhost:8080/metrics)
    WAIT_TIMEOUT    Wait timeout in seconds (default: 30)

Examples:
    $0 start
    CONFIG_FILE=my-config.yaml $0 start
    $0 logs
    $0 stop
EOF
}

# Main execution
main() {
    local command="${1:-start}"

    case "$command" in
        start)
            check_binary || exit 1
            check_config || exit 1

            # Stop any existing instance
            if [ -f kminion.pid ]; then
                log_warn "KMinion already running, stopping it first..."
                stop_kminion
            fi

            local pid
            pid=$(start_kminion)

            if wait_for_kminion "$pid"; then
                log_info "KMinion is running with PID: $pid"
                exit 0
            else
                stop_kminion
                exit 1
            fi
            ;;
        stop)
            stop_kminion
            ;;
        restart)
            stop_kminion
            sleep 2
            exec "$0" start
            ;;
        status)
            if [ -f kminion.pid ]; then
                local pid
                pid=$(cat kminion.pid)
                if kill -0 "$pid" 2>/dev/null; then
                    log_info "KMinion is running (PID: $pid)"
                    if curl -sf "$METRICS_URL" > /dev/null 2>&1; then
                        log_info "Metrics endpoint is responding"
                    else
                        log_warn "Metrics endpoint is not responding"
                    fi
                    exit 0
                else
                    log_warn "PID file exists but process is not running"
                    exit 1
                fi
            else
                log_warn "KMinion is not running"
                exit 1
            fi
            ;;
        logs)
            if [ -f "$LOG_FILE" ]; then
                tail -f "$LOG_FILE"
            else
                log_error "Log file not found: $LOG_FILE"
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

