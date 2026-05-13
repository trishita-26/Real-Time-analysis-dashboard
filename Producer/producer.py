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

products = [
    {
        "id": i,
        "name": random.choice([f"Product_{i}", f"product_{i}", f"PRODUCT {i}", f"Prod-{i}", None]),
        "category": random.choice(["Electronics", "Fashion", "Home", "Gold", "HealthCare"]),
        "price": random.randint(500, 80000)
    }
    for i in range(1, 101)
]

events = ["view", "add_to_cart", "purchase"]
cities = ["Delhi", "Mumbai", "Kolkata", "Bangalore"]

# --- Real-world anomaly generators ---

def maybe_null(value, prob=0.05):
    """Randomly drop fields — sensors time out, frontend bugs skip fields."""
    return None if random.random() < prob else value

def corrupt_timestamp():
    """Real systems produce: future timestamps, epoch 0, timezone-naive, malformed strings."""
    r = random.random()
    if r < 0.03:
        return "1970-01-01T00:00:00"                          # epoch zero / uninitialized
    elif r < 0.06:
        return (datetime.now() + timedelta(hours=random.randint(1, 72))).isoformat()  # future ts (clock skew)
    elif r < 0.09:
        return datetime.now().strftime("%d-%m-%Y %H:%M")      # wrong format (region mismatch)
    elif r < 0.11:
        return "NaT"                                          # pandas artifact leaked into stream
    else:
        return datetime.now().isoformat()

def corrupt_price(base_price):
    """Negative prices, zero, string prices, astronomical outliers."""
    r = random.random()
    if r < 0.02: return -base_price                           # refund logged as negative price
    if r < 0.04: return 0                                     # free / promo not handled
    if r < 0.06: return base_price * 1000                     # currency unit mismatch (paise vs rupees)
    if r < 0.08: return str(base_price) + " INR"              # price as string with unit embedded
    if r < 0.09: return None                                   # price field missing
    return base_price

def corrupt_city(city):
    """Typos, encoding errors, inconsistent casing, legacy codes."""
    r = random.random()
    if r < 0.04: return city.upper()                          # DELHI instead of Delhi
    if r < 0.07: return city.lower()                          # delhi
    if r < 0.09: return random.choice(["Dilli", "Bombay", "Calcutta", "BLR", "MUM", "DEL"])
    if r < 0.11: return None                                   # VPN user, geo-lookup failed
    if r < 0.12: return "???"                                  # encoding failure
    return city

def bot_or_duplicate(data):
    """
    Bots: same user_id hammers 'view' rapidly, quantity=0 or absurdly high.
    Duplicates: network retry sends same order_id twice.
    """
    r = random.random()
    if r < 0.04:
        # Bot signature: user_id in a narrow bot range, quantity=0, rapid views
        data["user_id"] = random.randint(9000, 9020)          # bot farm IDs cluster
        data["event_type"] = "view"
        data["quantity"] = 0
        data["_anomaly"] = "bot_traffic"
    elif r < 0.07:
        # Duplicate event — same order_id resent due to at-least-once delivery
        data["order_id"] = 1111                                # fixed ID = obvious duplicate
        data["_anomaly"] = "duplicate_retry"
    elif r < 0.09:
        data["quantity"] = random.randint(500, 9999)           # absurd quantity (data entry error)
        data["_anomaly"] = "qty_outlier"
    return data

def schema_drift(data):
    """
    Old app versions send different field names.
    New fields appear without warning.
    Some fields vanish entirely.
    """
    r = random.random()
    if r < 0.04:
        data["prod_id"] = data.pop("product_id")               # renamed key from old service version
        data["_anomaly"] = "schema_v1_legacy"
    elif r < 0.06:
        data["user_uuid"] = str(uuid.uuid4())                  # new field added by another team
        data["session_id"] = str(uuid.uuid4())
    elif r < 0.08:
        data.pop("payment_method", None)                       # field dropped in mobile app v3.2
        data["_anomaly"] = "missing_payment_field"
    elif r < 0.09:
        data["event_type"] = random.choice(["VIEW", "AddToCart", "BUY", "checkout_complete"])
        data["_anomaly"] = "event_name_inconsistency"          # multiple teams, no standard
    return data

def mixed_user_id(uid):
    """Some services send int user_id, others send UUID strings after a platform migration."""
    if random.random() < 0.08:
        return str(uuid.uuid4())                               # post-migration UUID format
    return uid

# --- Main producer loop ---

seen_order_ids = set()

while True:
    product = random.choice(products)

    base_order_id = random.randint(1000, 9999)
    user_id = random.randint(1, 500)

    data = {
        "order_id":       maybe_null(base_order_id, prob=0.02),
        "user_id":        mixed_user_id(user_id),
        "product_id":     maybe_null(product["id"], prob=0.03),
        "product_name":   maybe_null(product["name"], prob=0.05),
        "category":       maybe_null(product["category"], prob=0.04),
        "price":          corrupt_price(product["price"]),
        "quantity":       maybe_null(random.randint(1, 3), prob=0.03),
        "event_type":     random.choice(events),
        "timestamp":      corrupt_timestamp(),
        "payment_method": maybe_null(random.choice(["UPI", "Card", "COD"]), prob=0.06),
        "city":           corrupt_city(random.choice(cities)),
    }

    data = bot_or_duplicate(data)
    data = schema_drift(data)

    # Simulate network jitter and backpressure — variable delay
    delay = random.choice([
        0.05,                          # burst
        0.5,                           # normal
        random.uniform(2.0, 8.0),      # lag spike (DB slow, GC pause)
        0,                             # fire-and-forget flood
    ])

    # Occasional silent message loss (producer didn't flush)
    if random.random() > 0.97:
        print(f"[DROPPED] Message silently lost — producer buffer full")
        time.sleep(delay)
        continue

    producer.send("ecommerce_topic", data)
    print(data)

    # Burst pattern: flash sales trigger 20–200 events in seconds
    if random.random() < 0.03:
        print("[BURST] Flash sale detected — flooding topic")
        for _ in range(random.randint(20, 200)):
            producer.send("ecommerce_topic", data)

    time.sleep(0.05)