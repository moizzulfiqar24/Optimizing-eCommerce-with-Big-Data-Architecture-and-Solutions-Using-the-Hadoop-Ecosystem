#!/bin/bash

# Kafka topic name
TOPIC="OrderTopic"

# Path to the CSV file
CSV_FILE="/orders.csv"

# Track the current line number
START_LINE=1

while true; do
    # Get the next 50 rows, skipping already processed rows
    END_LINE=$((START_LINE + 49))
    sed -n "${START_LINE},${END_LINE}p" "$CSV_FILE" | while IFS= read -r row; do
        # Publish each row to the Kafka topic
        echo "$row" | /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic "$TOPIC"
    done

    # Update the start line for the next batch
    START_LINE=$((END_LINE + 1))

    # If the file has been completely read, reset to the beginning
    TOTAL_LINES=$(wc -l < "$CSV_FILE")
    if [ "$START_LINE" -gt "$TOTAL_LINES" ]; then
        START_LINE=1
    fi

    # Sleep for 5 minutes before the next batch
    sleep 300
done
