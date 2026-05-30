import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# FIX 1: Products list only stores id and base_price now.
# Name and category are chosen fresh inside the loop each time
# so every message is genuinely random, not pre-decided at startup.
products = [
    {"id": i, "price": random.randint(500, 80000)}
    for i in range(1, 101)
]

CATEGORIES   = ["Electronics", "Fashion", "Home", "Gold", "HealthCare"]
CITIES       = ["Delhi", "Mumbai", "Kolkata", "Bangalore"]
EVENTS       = ["view", "add_to_cart", "purchase"]
PAYMENT_METHODS = ["UPI", "Card", "COD"]

# --- Real-world anomaly generators ---

def maybe_null(value, prob=0.05):
    """Randomly drop fields — sensors time out, frontend bugs skip fields."""
    return None if random.random() < prob else value

def corrupt_timestamp():
    """Real systems produce: future timestamps, epoch 0, timezone-naive, malformed strings."""
    r = random.random()
    if r < 0.03:
        return "1970-01-01T00:00:00"
    elif r < 0.06:
        return (datetime.now() + timedelta(hours=random.randint(1, 72))).isoformat()
    elif r < 0.09:
        return datetime.now().strftime("%d-%m-%Y %H:%M")
    elif r < 0.11:
        return "NaT"
    else:
        return datetime.now().isoformat()

def corrupt_price(base_price):
    """Negative prices, zero, string prices, astronomical outliers."""
    r = random.random()
    if r < 0.02: return -base_price
    if r < 0.04: return 0
    if r < 0.06: return base_price * 1000
    if r < 0.08: return str(base_price) + " INR"
    if r < 0.09: return None
    return base_price

def corrupt_city(city):
    """Typos, encoding errors, inconsistent casing, legacy codes."""
    r = random.random()
    if r < 0.04: return city.upper()
    if r < 0.07: return city.lower()
    if r < 0.09: return random.choice(["Dilli", "Bombay", "Calcutta", "BLR", "MUM", "DEL"])
    if r < 0.11: return None
    if r < 0.12: return "???"
    return city

def bot_or_duplicate(data):
    """Bots, duplicates, absurd quantities."""
    r = random.random()
    if r < 0.04:
        data["user_id"]   = random.randint(9000, 9020)
        data["event_type"] = "view"
        data["quantity"]  = 0
        data["_anomaly"]  = "bot_traffic"
    elif r < 0.07:
        data["order_id"]  = 1111
        data["_anomaly"]  = "duplicate_retry"
    elif r < 0.09:
        data["quantity"]  = random.randint(500, 9999)
        data["_anomaly"]  = "qty_outlier"
    return data

def schema_drift(data):
    """Old app versions, new fields, dropped fields."""
    r = random.random()
    if r < 0.04:
        data["prod_id"] = data.pop("product_id")
        data["_anomaly"] = "schema_v1_legacy"
    elif r < 0.06:
        data["user_uuid"]   = str(uuid.uuid4())
        data["session_id"]  = str(uuid.uuid4())
    elif r < 0.08:
        data.pop("payment_method", None)
        data["_anomaly"] = "missing_payment_field"
    elif r < 0.09:
        data["event_type"] = random.choice(["VIEW", "AddToCart", "BUY", "checkout_complete"])
        data["_anomaly"]   = "event_name_inconsistency"
    return data

def mixed_user_id(uid):
    """Some services send int user_id, others send UUID strings."""
    if random.random() < 0.08:
        return str(uuid.uuid4())
    return uid

# --- Main producer loop ---

try:
    while True:
        product = random.choice(products)

        # FIX 1 (continued): name and category chosen fresh here every loop
        product_name = maybe_null(
            random.choice([
                f"Product_{product['id']}",
                f"product_{product['id']}",
                f"PRODUCT {product['id']}",
                f"Prod-{product['id']}"
            ]),
            prob=0.05
        )
        category = maybe_null(random.choice(CATEGORIES), prob=0.04)

        base_order_id = random.randint(1000, 9999)
        user_id       = random.randint(1, 500)

        data = {
            "order_id":       maybe_null(base_order_id, prob=0.02),
            "user_id":        mixed_user_id(user_id),
            "product_id":     maybe_null(product["id"], prob=0.03),
            "product_name":   product_name,
            "category":       category,
            "price":          corrupt_price(product["price"]),
            "quantity":       maybe_null(random.randint(1, 3), prob=0.03),
            "event_type":     random.choice(EVENTS),
            "timestamp":      corrupt_timestamp(),
            "payment_method": maybe_null(random.choice(PAYMENT_METHODS), prob=0.06),
            "city":           corrupt_city(random.choice(CITIES)),
        }

        data = bot_or_duplicate(data)
        data = schema_drift(data)

        delay = random.choice([0.05, 0.5, random.uniform(2.0, 8.0), 0])

        if random.random() > 0.97:
            print(f"[DROPPED] Message silently lost — producer buffer full")
            time.sleep(delay)
            continue

        producer.send("ecommerce_topic", data)
        print(data)

        # FIX 2: burst loop now generates a fresh varied message each time
        # instead of re-sending the exact same mutated dict 200 times
        if random.random() < 0.03:
            print("[BURST] Flash sale detected — flooding topic")
            for _ in range(random.randint(20, 200)):
                burst_data = {
                    **data,
                    "order_id": random.randint(1000, 9999),  # vary order_id
                    "user_id":  random.randint(1, 500),       # vary user
                }
                producer.send("ecommerce_topic", burst_data)

        time.sleep(0.05)

# FIX 3: flush and close cleanly on Ctrl+C
# Without this, messages buffered in KafkaProducer memory are lost on exit
except KeyboardInterrupt:
    print("\n[PRODUCER] Shutting down cleanly...")
finally:
    producer.flush()   # send all buffered messages before closing
    producer.close()
    print("[PRODUCER] Closed.")