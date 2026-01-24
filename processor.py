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

logger.info(f"Processor started. Monitoring '{SOURCE_TOPIC}'...")

try:
    while True:
        now = time.time()
        if now - window_start >= WINDOW_SIZE_SEC:
            if counts:
                ts_start = datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat()
                logger.info(f"Window expired. Emitting {len(counts)} zone aggregates...")

                for zone, count in counts.items():
                    metric_payload = {
                        "pickup_zone": zone,
                        "ride_count": count,
                        "location": locations[zone],
                        "window_start": ts_start,
                        "processed_at": datetime.now(timezone.utc).isoformat()
                    }
                    
                    producer.produce(
                        SINK_TOPIC,
                        key=zone,
                        value=json.dumps(metric_payload).encode("utf-8"),
                        callback=delivery_report
                    )

                producer.flush()
                
                counts.clear()
                locations.clear()
            
            window_start = now

        msg = consumer.poll(0.5) 
        
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        try:
            raw_val = msg.value().decode("utf-8")
            event = json.loads(raw_val)
            
            lat = float(event.get("latitude") or event.get("lat", 0))
            lon = float(event.get("longitude") or event.get("lon", 0))
            
            if lat == 0 or lon == 0:
                continue

            pickup_zone = f"{round(lat, 3)}_{round(lon, 3)}"
            
            counts[pickup_zone] += 1
            locations[pickup_zone] = {"lat": lat, "lon": lon}
            
            if sum(counts.values()) % 100 == 0:
                logger.info(f"Buffered {sum(counts.values())} rides in current window...")

        except Exception as e:
            logger.warning(f"Skipping malformed message: {e}")
            continue
