from kafka import KafkaConsumer
import json
import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres123",
    host="localhost",
    port=5433
)
cur = conn.cursor()

consumer = KafkaConsumer(
    'ecommerce_topic',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")
for message in consumer:
    data = message.value
    cur.execute("""
        INSERT INTO raw_data
        (order_id, user_id, product_id, product_name,
         category, price, quantity, event_type,
         timestamp, payment_method, city)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (data['order_id'], data['user_id'], data['product_id'],
         data['product_name'], data['category'], data['price'],
         data['quantity'], data['event_type'], data['timestamp'],
         data['payment_method'], data['city']))
    conn.commit()
    print(f"Inserted order {data['order_id']}")