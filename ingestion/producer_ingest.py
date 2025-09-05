# requires: aiokafka, aiohttp (for scraping), ujson
import asyncio, uuid, datetime, json
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "news-headlines"

async def produce_example():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        event = {
            "event_id": str(uuid.uuid4()),
            "type": "news",
            "title": "Fed signals a possible slowdown",
            "body": "Text summary or full article text",
            "url": "https://example.com/article",
            "source": "reuters",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        await producer.send_and_wait(TOPIC, json.dumps(event).encode())
        print("sent")
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce_example())
