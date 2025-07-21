#!/bin/bash

# Usage: 
# REGION=AMER ./app_write.sh 
# REGION=EMEA ./app_write.sh

set -e

# Load environment variables from .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Graceful shutdown on Ctrl+C
cleanup() {
  echo "\nReceived Ctrl+C. Exiting gracefully..."
  exit 0
}
trap cleanup SIGINT

ENVIRONMENT=${ENVIRONMENT:-dev}
REGION=${REGION:-AMER} # AMER or EMEA, defaults to AMER
BATCH_SIZE=1000
TMPFILE="./mongo_batch_${REGION}.json"
LOGFILE="./mongo_batch_${REGION}.log"
# Remove the hardcoded MONGO_URI assignment
DB_NAME="aaDB"
COLLECTION="aaColl"

# Function to hash a string and return 0 if even, 1 if odd
hash_parity() {
  local input="$1"
  local hash
  hash=$(echo -n "$input" | md5sum | cut -d' ' -f1)
  local hex_first8=${hash:0:8}
  local dec=$((16#$hex_first8))
  if (( dec % 2 == 0 )); then
    echo "even"
  else
    echo "odd"
  fi
}

# Start clean temp file and begin logging
> "$TMPFILE"
echo "=== Starting new session: $(date) ===" >> "$LOGFILE"
count=0

# Main stream processing with resiliency
while true; do
  echo "[INFO] Starting/restarting Wikimedia stream at $(date)" >&2
  curl -s https://stream.wikimedia.org/v2/stream/recentchange | \
  grep data | \
  sed 's/^data: //g' | \
  jq -rc 'with_entries(if .key == "$schema" then .key = "schema" else . end)' | \
  while read -r line; do
    title_url=$(echo "$line" | jq -r '.meta.uri')
    parity=$(hash_parity "$title_url")

    case "$REGION:$parity" in
      AMER:even)
        enriched=$(echo "$line" | jq '. + {location: "US"}')
        ;;
      EMEA:odd)
        enriched=$(echo "$line" | jq '. + {location: "DE"}')
        ;;
      *)
        continue
        ;;
    esac

    echo "$enriched" >> "$TMPFILE"
    ((count++))

    if (( count >= BATCH_SIZE )); then
      echo "$(date) — Importing batch of $count documents for REGION=$REGION..." >> "$LOGFILE"

      jq -s '.' "$TMPFILE" > "$TMPFILE.arr"

      REGION="$REGION" python3 mongo_batch_writer.py "$MONGO_URI" "$DB_NAME" "$COLLECTION" >> "$LOGFILE" 2>&1

      if [[ $? -eq 0 ]]; then
        echo "$(date) — ✅ Import successful (PyMongo with retryable writes)." >> "$LOGFILE"
      else
        echo "$(date) — ❌ Import failed!" >> "$LOGFILE"
      fi

      > "$TMPFILE"
      count=0
    fi
  done
  echo "[WARN] Stream disconnected. Reconnecting in 5 seconds..." >&2
  sleep 5
done