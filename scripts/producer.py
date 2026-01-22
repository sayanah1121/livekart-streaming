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
            print(f"⏳ Waiting for Kafka... ({retries}/15)")
            time.sleep(5)
            retries += 1
    raise Exception(" Kafka unreachable.")

def generate_stream():
    producer = create_producer()
    topic = "livekart_orders"
    print(f" Streaming rich data to {topic}...")
    
    categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Toys']
    
    try:
        while True:
            # IMPROVED DATA GENERATION
            category = random.choice(categories)
            order = {
                "order_id": f"ORD-{random.randint(10000, 99999)}",
                # Added User ID
                "user_id": f"USR-{random.randint(1, 100)}", 
                # Added Product ID
                "product_id": f"PROD-{category[:3].upper()}-{random.randint(100, 999)}",
                "category": category,
                "price": round(random.uniform(20, 500), 2),
                # Added Quantity
                "quantity": random.randint(1, 5),
                "timestamp": str(int(time.time()))
            }
            
            producer.send(topic, value=order)
            print(f"Sent: {order['order_id']} | User: {order['user_id']} | Item: {order['product_id']}")
            time.sleep(1)
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    generate_stream()