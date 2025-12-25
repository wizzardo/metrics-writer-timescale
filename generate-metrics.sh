#!/bin/bash

# Configuration
URL=${1:-"http://localhost:8080/v1/metrics"}
COUNT=${2:-10}
INTERVAL_MS=${3:-1000}

echo "Generating $COUNT metrics batches to $URL..."

for i in $(seq 1 $COUNT); do
    TIMESTAMP=$(date +%s%N)
    
    VAL1=$(awk "BEGIN {print 20 + rand() * 10}")
    VAL2=$(awk "BEGIN {print 50 + rand() * 20}")
    
    JSON_DATA=$(cat <<EOF
[
  {
    "name": "cpu_load",
    "value": $VAL1,
    "tags": [["host", "server-$((i % 3))"], ["region", "us-west"], ["group", "test"]],
    "timestamp": $TIMESTAMP
  },
  {
    "name": "mem_usage",
    "value": $VAL2,
    "tags": [["host", "server-$((i % 3))"], ["env", "production"]],
    "timestamp": $TIMESTAMP
  }
]
EOF
)

    curl -s -X POST -H "Content-Type: application/json" -d "$JSON_DATA" "$URL"
    echo "Batch $i sent."
    
    if [ "$i" -lt "$COUNT" ]; then
        sleep $(awk "BEGIN {print $INTERVAL_MS / 1000}")
    fi
done

echo "Finished."
