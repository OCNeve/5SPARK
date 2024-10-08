#!/bin/sh
# Wait for Kafka to start (you can adjust the sleep duration based on your startup time)
sleep 5
# Create Kafka topic
kafka-topics --create --topic mastodonStream --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1