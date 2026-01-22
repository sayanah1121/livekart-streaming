import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_producer():
    retries = 0
    while retries < 15:
        try:
            return KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except NoBrokersAvailable:
            print(f" Waiting for Kafka... ({retries}/15)")
            time.sleep(5)
            retries += 1
    raise Exception(" Kafka unreachable.")

def generate_stream():
    producer = create_producer()
    topic = "livekart_orders"
    print(f" Streaming to {topic}...")
    
    categories = ['Electronics', 'Fashion', 'Home', 'Books']
    try:
        while True:
            order = {
                "order_id": f"ORD-{random.randint(10000, 99999)}",
                "category": random.choice(categories),
                "price": round(random.uniform(20, 500), 2),
                "timestamp": str(int(time.time()))
            }
            producer.send(topic, value=order)
            print(f"Sent: {order['order_id']} | ${order['price']}")
            time.sleep(1)
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    generate_stream()