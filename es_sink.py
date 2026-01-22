import json
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch

consumer = Consumer({
    "bootstrap.servers": "localhost:9095,localhost:9097,localhost:9102",
    "group.id": "taxi-es-sink",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["taxi-metrics"])

es = Elasticsearch("http://localhost:9200")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    document = json.loads(msg.value().decode("utf-8"))
    es.index(index="taxi-metrics", document=document)
