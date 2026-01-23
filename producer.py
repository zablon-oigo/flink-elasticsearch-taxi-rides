import gzip
import csv
import json
import time
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9095,localhost:9097,localhost:9102"
})

def delivery_report(err, msg):
    """Called once for each message to indicate delivery result."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

columns = ["ride_id", "timestamp", "event", "longitude", "latitude", "passenger_count", "other_field"]

with gzip.open("data/nycTaxiData.gz", "rt") as f:
    reader = csv.DictReader(f, fieldnames=columns)
    for row in reader:
        row["ride_id"] = int(row["ride_id"])
        row["longitude"] = float(row["longitude"])
        row["latitude"] = float(row["latitude"])
        row["passenger_count"] = int(row["passenger_count"])

        producer.produce(
            "taxi-rides",
            value=json.dumps(row).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.01)

producer.flush()
