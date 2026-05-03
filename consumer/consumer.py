from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ecommerce_topic',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    data = message.value
    print(data)