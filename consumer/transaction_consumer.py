
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

from kafka import KafkaConsumer

from config.config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC, CONSUMER_GROUP


consumer = KafkaConsumer(
    TRANSACTION_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda key: key.decode("utf-8") if key else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


print("Consumer started...")
print(f"Listening to topic: {TRANSACTION_TOPIC}")
print(f"Consumer group: {CONSUMER_GROUP}")

for message in consumer:
    transaction = message.value

    print(
        f"Received: {transaction['transaction_id']} | "
        f"Card: {transaction['card_id']} | "
        f"Amount: ₹{transaction['amount']:.2f}"
    )