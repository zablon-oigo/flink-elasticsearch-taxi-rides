import json
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9095,localhost:9097,localhost:9102",
    "group.id": "taxi-processor-v2", 
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}

SOURCE_TOPIC = "taxi-rides"
SINK_TOPIC = "taxi-metrics"
WINDOW_SIZE_SEC = 10  
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([SOURCE_TOPIC])
producer = Producer({"bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"]})

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Metric sent to {msg.topic()} [Partition: {msg.partition()}]")

window_start = time.time()
counts = defaultdict(int)
locations = defaultdict(dict)

