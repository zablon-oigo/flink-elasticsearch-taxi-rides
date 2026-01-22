import csv
import json
import time
import gzip
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9095,localhost:9097,localhost:9102"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

with gzip.open("data/nycTaxiData.gz", "rt") as f:
    reader = csv.DictReader(f)

    for row in reader:
        producer.produce(
            topic="taxi-rides",
            value=json.dumps(row).encode("utf-8"),
            on_delivery=delivery_report
        )
        producer.poll(0)  
        time.sleep(0.05)

producer.flush()
