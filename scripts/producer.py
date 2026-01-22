import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

# --- CONFIGURATION ---
KAFKA_TOPIC = "livekart_orders"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Fake Data Generator
fake = Faker()

# Define Product Categories & Prices
PRODUCT_CATALOG = {
    "Electronics": [("Laptop", 1200), ("Smartphone", 800), ("Headphones", 150)],
    "Clothing": [("T-Shirt", 25), ("Jeans", 60), ("Sneakers", 90)],
    "Home": [("Blender", 45), ("Lamp", 30), ("Coffee Maker", 85)]
}

def create_order():
    """Generates a random e-commerce order."""
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    product_name, base_price = random.choice(PRODUCT_CATALOG[category])
    
    # Add some randomness to price (e.g., discount or tax)
    final_price = round(base_price * random.uniform(0.9, 1.1), 2)
    
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.user_name(),
        "product": product_name,
        "category": category,
        "price": final_price,
        "timestamp": int(time.time()), # Epoch time
        "payment_method": random.choice(["Credit Card", "PayPal", "UPI", "Debit Card"])
    }

def start_producer():
    print(f" Starting LiveKart Order Stream to topic: {KAFKA_TOPIC}...")
    
    # Connect to Kafka
    # value_serializer: Automatically turns our Dictionary into JSON Bytes
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f" Error connecting to Kafka: {e}")
        print(" TIP: Is Docker running? Is Kafka mapped to port 9092?")
        return

    try:
        while True:
            order = create_order()
            
            # SEND TO KAFKA
            # We send the data asynchronously (fire and forget)
            producer.send(KAFKA_TOPIC, order)
            
            print(f"Sent: {order['product']} (${order['price']}) -> {order['user_id']}")
            
            # Simulate real-time gap (1 order every 0.5 to 2 seconds)
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n Stopping Producer.")
        producer.close()

if __name__ == "__main__":
    start_producer()