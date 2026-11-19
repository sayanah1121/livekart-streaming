import argparse
import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_producer(bootstrap):
    retries = 0
    while retries < 15:
        try:
            return KafkaProducer(
                bootstrap_servers=[bootstrap],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                linger_ms=5,
                batch_size=64 * 1024,
            )
        except NoBrokersAvailable:
            print(f"⏳ Waiting for Kafka... ({retries}/15)")
            time.sleep(5)
            retries += 1
    raise Exception(" Kafka unreachable.")

CATEGORIES = ['Electronics', 'Fashion', 'Home', 'Books', 'Toys']

def make_order():
    category = random.choice(CATEGORIES)
    return {
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "user_id": f"USR-{random.randint(1, 100)}",
        "product_id": f"PROD-{category[:3].upper()}-{random.randint(100, 999)}",
        "category": category,
        "price": round(random.uniform(20, 500), 2),
        "quantity": random.randint(1, 5),
        "timestamp": str(int(time.time()))
    }

def generate_stream(rate, count, bootstrap, topic, verbose):
    """rate: target events/sec (0 = as fast as possible). count: stop after N events (0 = forever)."""
    producer = create_producer(bootstrap)
    print(f" Streaming to {topic} | target rate: {rate or 'max'} ev/s | count: {count or 'unbounded'}")

    sent = 0
    start = time.perf_counter()
    last_report = start
    try:
        while count == 0 or sent < count:
            order = make_order()
            producer.send(topic, value=order)
            sent += 1
            if verbose:
                print(f"Sent: {order['order_id']} | User: {order['user_id']} | Item: {order['product_id']}")
            if rate:
                # Pace against wall clock so the average rate holds even if sends jitter
                target = start + sent / rate
                delay = target - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            now = time.perf_counter()
            if now - last_report >= 5:
                print(f"  {sent:,} sent | {sent / (now - start):,.0f} ev/s")
                last_report = now
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        elapsed = time.perf_counter() - start
        producer.close()
        print(f"DONE sent={sent} seconds={elapsed:.2f} rate={sent / elapsed:.1f} ev/s")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="LiveKart order event producer")
    p.add_argument("--rate", type=float, default=1, help="events per second; 0 = unthrottled (default 1)")
    p.add_argument("--count", type=int, default=0, help="stop after N events; 0 = run forever (default)")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="livekart_orders")
    p.add_argument("--quiet", action="store_true", help="don't print every event")
    a = p.parse_args()
    generate_stream(a.rate, a.count, a.bootstrap, a.topic, verbose=not a.quiet)
