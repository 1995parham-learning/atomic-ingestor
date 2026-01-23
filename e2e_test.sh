#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test directories
TEST_DIR=$(mktemp -d)
INPUT_DIR="$TEST_DIR/input"
WAREHOUSE_DIR="$TEST_DIR/warehouse"
MANIFESTS_DIR="$TEST_DIR/manifests"
STATE_DB="$TEST_DIR/state.db"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    if [ -n "$INGESTOR_PID" ] && kill -0 "$INGESTOR_PID" 2>/dev/null; then
        kill "$INGESTOR_PID" 2>/dev/null || true
        wait "$INGESTOR_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT

# Helper functions
log_step() {
    echo -e "\n${YELLOW}=== $1 ===${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_error() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

wait_for_file() {
    local file="$1"
    local timeout="${2:-10}"
    local elapsed=0
    while [ ! -f "$file" ] && [ $elapsed -lt $timeout ]; do
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    [ -f "$file" ]
}

wait_for_file_gone() {
    local file="$1"
    local timeout="${2:-10}"
    local elapsed=0
    while [ -f "$file" ] && [ $elapsed -lt $timeout ]; do
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    [ ! -f "$file" ]
}

# Create test directories
log_step "Setting up test environment"
mkdir -p "$INPUT_DIR" "$WAREHOUSE_DIR" "$MANIFESTS_DIR"
echo "Test directory: $TEST_DIR"
log_success "Test directories created"

# Build the project
log_step "Building atomic-ingestor"
go build -o "$TEST_DIR/atomic-ingestor" . || log_error "Build failed"
log_success "Build successful"

# Run unit tests first
log_step "Running unit tests"
go test -v -short ./... || log_error "Unit tests failed"
log_success "Unit tests passed"

# Start the ingestor in sidecar mode
log_step "Starting atomic-ingestor (sidecar mode)"
"$TEST_DIR/atomic-ingestor" \
    --input "$INPUT_DIR" \
    --warehouse "$WAREHOUSE_DIR" \
    --manifests "$MANIFESTS_DIR" \
    --state-path "$STATE_DB" \
    --mode sidecar \
    --log-level debug &
INGESTOR_PID=$!
sleep 2

if ! kill -0 "$INGESTOR_PID" 2>/dev/null; then
    log_error "Ingestor failed to start"
fi
log_success "Ingestor started (PID: $INGESTOR_PID)"

# Test 1: Basic file ingestion with sidecar
log_step "Test 1: Basic file ingestion with sidecar"
TEST_CONTENT_1="id,name,value
1,test1,100
2,test2,200"
echo "$TEST_CONTENT_1" > "$INPUT_DIR/test1.csv"
touch "$INPUT_DIR/test1.csv.ok"

if wait_for_file_gone "$INPUT_DIR/test1.csv" 15; then
    log_success "File moved from input directory"
else
    log_error "File was not moved from input directory"
fi

if wait_for_file "$WAREHOUSE_DIR/test1.csv" 5; then
    log_success "File appeared in warehouse"
else
    log_error "File did not appear in warehouse"
fi

# Verify content
if diff <(echo "$TEST_CONTENT_1") "$WAREHOUSE_DIR/test1.csv" > /dev/null; then
    log_success "File content matches"
else
    log_error "File content mismatch"
fi

# Test 2: Manifest creation
log_step "Test 2: Verifying manifest creation"
sleep 1
MANIFEST_FILE=$(find "$MANIFESTS_DIR" -name "manifest.jsonl" -type f 2>/dev/null | head -1)
if [ -n "$MANIFEST_FILE" ] && [ -f "$MANIFEST_FILE" ]; then
    log_success "Manifest file created: $MANIFEST_FILE"
    echo "Manifest content:"
    cat "$MANIFEST_FILE"
else
    log_error "Manifest file not created"
fi

# Verify manifest contains SHA256
if grep -q "sha256" "$MANIFEST_FILE"; then
    log_success "Manifest contains SHA256 hash"
else
    log_error "Manifest missing SHA256 hash"
fi

# Test 3: Deduplication (same content, different filename)
log_step "Test 3: Deduplication test"
echo "$TEST_CONTENT_1" > "$INPUT_DIR/test1_duplicate.csv"
touch "$INPUT_DIR/test1_duplicate.csv.ok"
sleep 3

if wait_for_file_gone "$INPUT_DIR/test1_duplicate.csv" 10; then
    # File should be removed but NOT appear in warehouse (deduplicated)
    if [ ! -f "$WAREHOUSE_DIR/test1_duplicate.csv" ]; then
        log_success "Duplicate file correctly skipped (not in warehouse)"
    else
        log_error "Duplicate file should not have been moved to warehouse"
    fi
else
    log_error "Duplicate file was not processed"
fi

# Test 4: Multiple files
log_step "Test 4: Multiple file ingestion"
for i in 2 3 4; do
    echo "id,name,value
$i,test$i,$((i * 100))" > "$INPUT_DIR/batch_$i.csv"
    touch "$INPUT_DIR/batch_$i.csv.ok"
done

sleep 5
PROCESSED=0
for i in 2 3 4; do
    if [ -f "$WAREHOUSE_DIR/batch_$i.csv" ]; then
        PROCESSED=$((PROCESSED + 1))
    fi
done

if [ $PROCESSED -eq 3 ]; then
    log_success "All 3 batch files processed"
else
    log_error "Only $PROCESSED/3 batch files were processed"
fi

# Test 5: Subdirectory support
log_step "Test 5: Subdirectory file ingestion"
mkdir -p "$INPUT_DIR/subdir"
echo "nested,file,data" > "$INPUT_DIR/subdir/nested.csv"
touch "$INPUT_DIR/subdir/nested.csv.ok"
sleep 3

if [ -f "$WAREHOUSE_DIR/subdir/nested.csv" ]; then
    log_success "Nested file processed correctly"
else
    log_error "Nested file was not processed"
fi

# Test 6: Hidden files should be ignored
log_step "Test 6: Hidden file filtering"
echo "hidden,data" > "$INPUT_DIR/.hidden.csv"
touch "$INPUT_DIR/.hidden.csv.ok"
sleep 2

if [ -f "$INPUT_DIR/.hidden.csv" ]; then
    log_success "Hidden file correctly ignored"
else
    log_error "Hidden file should have been ignored"
fi

# Test 7: Temporary files should be ignored
log_step "Test 7: Temporary file filtering"
echo "temp,data" > "$INPUT_DIR/temp.csv.tmp"
sleep 2

if [ -f "$INPUT_DIR/temp.csv.tmp" ]; then
    log_success "Temporary file correctly ignored"
else
    log_error "Temporary file should have been ignored"
fi

# Test 8: Graceful shutdown
log_step "Test 8: Graceful shutdown"
kill -TERM "$INGESTOR_PID"
wait "$INGESTOR_PID" 2>/dev/null || true
log_success "Ingestor shut down gracefully"
INGESTOR_PID=""

# Test 9: Restart and verify state persistence
log_step "Test 9: State persistence after restart"
"$TEST_DIR/atomic-ingestor" \
    --input "$INPUT_DIR" \
    --warehouse "$WAREHOUSE_DIR" \
    --manifests "$MANIFESTS_DIR" \
    --state-path "$STATE_DB" \
    --mode sidecar \
    --log-level debug &
INGESTOR_PID=$!
sleep 2

# Try to add a file with same content as test1 - should be deduplicated
echo "$TEST_CONTENT_1" > "$INPUT_DIR/after_restart.csv"
touch "$INPUT_DIR/after_restart.csv.ok"
sleep 3

if [ ! -f "$WAREHOUSE_DIR/after_restart.csv" ]; then
    log_success "State persisted - duplicate still detected after restart"
else
    log_error "State not persisted - duplicate was processed after restart"
fi

# Test 10: Stability window mode
log_step "Test 10: Stability window mode"
kill -TERM "$INGESTOR_PID" 2>/dev/null || true
wait "$INGESTOR_PID" 2>/dev/null || true

# Start with stability window mode (short window for testing)
"$TEST_DIR/atomic-ingestor" \
    --input "$INPUT_DIR" \
    --warehouse "$WAREHOUSE_DIR" \
    --manifests "$MANIFESTS_DIR" \
    --state-path "$STATE_DB" \
    --mode stability_window \
    --stability-seconds 2 \
    --log-level debug &
INGESTOR_PID=$!
sleep 2

echo "stability,test,data" > "$INPUT_DIR/stability_test.csv"
# No .ok file needed in stability mode - wait for stability window
sleep 5

if [ -f "$WAREHOUSE_DIR/stability_test.csv" ]; then
    log_success "Stability window mode works correctly"
else
    log_error "Stability window mode failed"
fi

# Final summary
log_step "Test Summary"
echo -e "${GREEN}All end-to-end tests passed!${NC}"
echo ""
echo "Files in warehouse:"
find "$WAREHOUSE_DIR" -type f -name "*.csv" | while read f; do
    echo "  - $(basename "$f")"
done
echo ""
echo "Manifest entries:"
find "$MANIFESTS_DIR" -name "manifest.jsonl" -exec cat {} \; | wc -l | xargs echo "  Total entries:"
