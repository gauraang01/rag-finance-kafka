# Run topic creation script
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh

# Update topic

# Produce to topic
docker exec -it kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic news-headlines

# Consume to topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic news-headlines \
  --from-beginning

# Sample env file
NEWSAPI_KEY=your_api_key_here
KAFKA_BOOTSTRAP=localhost:29092

## 📰 News Producer (ingestion/producer_news.py)

The **news producer** ingests real-time articles from [NewsAPI](https://newsapi.org/) and publishes them into Kafka for downstream processing (vectorization, retrieval, RAG queries).

### 🔧 How it works
- Fetches articles for multiple queries (e.g., `Federal Reserve`, `Apple`, `Crypto`, `Stock Market`).
- Normalizes them into a consistent schema:
  - `schema_version`
  - `id` (URL hash for deduplication)
  - `source`
  - `query`
  - `title`, `description`, `content`
  - `url`
  - `published_at`
  - `fetched_at`
- Adds light cleanup and schema stamping.
- Produces messages to Kafka topic `news-headlines` with:
  - **Kafka key** = `<query>|<url_hash>`
  - **Idempotent producer enabled** for safe delivery.

Downstream stream processors or consumers (e.g., vectorizer, retrievers) can rely on this schema without extra preprocessing.

### ⚙️ Setup & Run

#### 1. Create Kafka topic
```bash
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
