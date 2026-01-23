import json
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch

consumer = Consumer({
    "bootstrap.servers": "localhost:9095,localhost:9097,localhost:9102",
    "group.id": "taxi-es-sink",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["taxi-metrics"])

es = Elasticsearch("http://localhost:9200")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        document = json.loads(msg.value().decode("utf-8"))

        doc_id = f"{document['pickup_zone']}_{document['window_start']}"

        try:
            es.index(
                index="taxi-metrics",
                id=doc_id,
                document=document
            )
            consumer.commit(msg)
        except Exception as e:
            print("Elasticsearch error:", e)

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
