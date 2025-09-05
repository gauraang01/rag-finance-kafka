#!/bin/bash

# Exit if any command fails
set -e

# Kafka broker host (inside docker-compose network)
BROKER="kafka:9092"

echo "📌 Creating Kafka topics..."

# Raw ingestion topics

docker exec -it kafka kafka-topics --create --if-not-exists \
  --bootstrap-server $BROKER \
  --replication-factor 1 --partitions 3 \
  --topic news-headlines

# docker exec -it kafka kafka-topics --create --if-not-exists \
#   --bootstrap-server $BROKER \
#   --replication-factor 1 --partitions 3 \
#   --topic news-headlines

# docker exec -it kafka kafka-topics --create --if-not-exists \
#   --bootstrap-server $BROKER \
#   --replication-factor 1 --partitions 3 \
#   --topic market-ticks

# docker exec -it kafka kafka-topics --create --if-not-exists \
#   --bootstrap-server $BROKER \
#   --replication-factor 1 --partitions 3 \
#   --topic economic-data

# # Processed / normalized topic from Kafka Streams
# docker exec -it kafka kafka-topics --create --if-not-exists \
#   --bootstrap-server $BROKER \
#   --replication-factor 1 --partitions 3 \
#   --topic events-for-vectorization

# # Optional: topic for vector indexing audit/logging
# docker exec -it kafka kafka-topics --create --if-not-exists \
#   --bootstrap-server $BROKER \
#   --replication-factor 1 --partitions 3 \
#   --topic vectors

# echo "✅ Topics created successfully!"
