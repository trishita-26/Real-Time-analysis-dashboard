import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [
    {"id": i, "name": f"Product_{i}", "category": random.choice(["Electronics", "Fashion", "Home","Gold", "HealthCare"]), "price": random.randint(500, 80000)}
    for i in range(1, 101)
]

events = ["view", "add_to_cart", "purchase"]
cities = ["Delhi", "Mumbai", "Kolkata", "Bangalore"]

while True:
    product = random.choice(products)

    data = {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 500),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 3),
        "event_type": random.choice(events),
        "timestamp": datetime.now().isoformat(),
        "payment_method": random.choice(["UPI", "Card", "COD"]),
        "city": random.choice(cities)
    }

    producer.send("ecommerce_topic", data)
    print(data)

for _ in range(random.randint(5, 15)):
    producer.send("ecommerce_topic", data)

time.sleep(0.5)