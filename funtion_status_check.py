from kafka import KafkaConsumer
import json

CARD_TOPIC = 'send_function_status'
brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']

consumer = KafkaConsumer(
    CARD_TOPIC,
    bootstrap_servers=brokers,
    value_deserializer=lambda m:json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f'{message.value}')