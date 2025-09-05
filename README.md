# Run topic creation script
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh

# Produce to topic
docker exec -it kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic news-headlines

# Consume to topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic news-headlines \
  --from-beginning

