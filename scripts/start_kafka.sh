#!/bin/bash

# Script to start Kafka (assuming Kafka is installed locally)
echo "Starting Kafka services..."

# Start Zookeeper (in background)
echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

# Wait for Zookeeper to start
sleep 10

# Start Kafka server (in background)
echo "Starting Kafka server..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# Wait for Kafka to start
sleep 15

# Create topics
echo "Creating Kafka topics..."
$KAFKA_HOME/bin/kafka-topics.sh --create --topic athlete_event_results --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --create --topic enriched_athlete_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka setup completed!"
echo "Topics created:"
echo "- athlete_event_results"
echo "- enriched_athlete_data"