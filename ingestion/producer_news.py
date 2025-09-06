from dotenv import load_dotenv
load_dotenv()

import asyncio
import os
import aiohttp
import datetime
import hashlib
import json
import uuid
import logging
from pathlib import Path
from aiokafka import AIOKafkaProducer
from jsonschema import validate, ValidationError
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
if not NEWSAPI_KEY:
    raise RuntimeError("Missing NEWSAPI_KEY environment variable")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "news-headlines"
BASE_URL = "https://newsapi.org/v2/everything"

# Load schema from file
BASE_DIR = Path(__file__).parent
SCHEMA_PATH = BASE_DIR / "schemas" / "news_v1.json"
with SCHEMA_PATH.open("r", encoding="utf-8") as f:
    NEWS_SCHEMA = json.load(f)


def normalize_article(article: dict, query: str) -> dict:
    """Normalize NewsAPI article into our schema (light cleanup + stamping)."""
    url = article.get("url", "")
    url_hash = hashlib.sha256(url.encode()).hexdigest() if url else str(uuid.uuid4())

    return {
        "schema_version": "1.0",
        "id": url_hash,  # deterministic ID for deduplication downstream
        "source": "newsapi",
        "query": query,
        "title": (article.get("title") or "").strip(),
        "description": (article.get("description") or "").strip(),
        "content": (article.get("content") or "").strip(),
        "url": url,
        "published_at": article.get("publishedAt"),
        "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
        "language": article.get("language", "en")
    }


async def fetch_news(session, query="Federal Reserve"):
    """Fetch articles for a given query from NewsAPI."""
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)

    url = (
        f"{BASE_URL}?q={query}"
        f"&from={week_ago}&to={today}"
        f"&sortBy=popularity&pageSize=50"
        f"&apiKey={NEWSAPI_KEY}"
    )

    async with session.get(url, headers={"User-Agent": "FinanceRAG/1.0"}) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"Failed request {resp.status}: {text}")
        return await resp.json()


async def produce_news():
    """Fetch and send news articles into Kafka with schema validation."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )
    await producer.start()

    queries = ["Federal Reserve", "Apple", "Crypto", "Stock Market"]

    try:
        async with aiohttp.ClientSession() as session:
            for query in queries:
                data = await fetch_news(session, query=query)
                articles = data.get("articles", [])
                logging.info(f"Retrieved {len(articles)} articles for query='{query}'")

                for article in articles:
                    event = normalize_article(article, query)

                    # Schema validation
                    try:
                        validate(instance=event, schema=NEWS_SCHEMA)
                    except ValidationError as e:
                        logging.warning(f"Schema validation failed: {e.message}")
                        continue

                    # Hybrid key: query + id
                    key = f"{query}|{event['id']}"
                    await producer.send_and_wait(TOPIC, value=event, key=key)
                    logging.info(f"Sent: {event['title']} [{query}]")

    finally:
        await producer.stop()
        logging.info("Kafka producer stopped.")


if __name__ == "__main__":
    asyncio.run(produce_news())
