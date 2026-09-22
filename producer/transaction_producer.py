import sys
import os
import json
import time
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer

from config.config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


cards = [
    "CARD1001",
    "CARD1002",
    "CARD1003",
    "CARD1004",
    "CARD1005"
]

locations = [
    "Hyderabad",
    "Bangalore",
    "Chennai",
    "Mumbai",
    "Delhi"
]

merchants = [
    "Amazon",
    "Walmart",
    "Flipkart",
    "Uber",
    "Restaurant"
]


def generate_realtime_transaction(transaction_number):
    card_id = random.choice(cards)

    # Mostly normal transactions, sometimes suspicious transactions
    is_suspicious = random.random() < 0.25

    if is_suspicious:
        amount = round(random.uniform(50000, 100000), 2)
    else:
        amount = round(random.uniform(100, 10000), 2)

    transaction = {
        "transaction_id": f"TXN{transaction_number:05d}",
        "card_id": card_id,
        "amount": amount,
        "location": random.choice(locations),
        "merchant": random.choice(merchants),
        "transaction_type": "PURCHASE",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    return transaction


def start_realtime_producer():
    transaction_number = 1

    print("========================================")
    print("   REAL-TIME CREDIT CARD PRODUCER")
    print("========================================")
    print("Sending transactions to Kafka...")
    print("Press Ctrl+C to stop.")
    print()

    try:
        while True:
            transaction = generate_realtime_transaction(transaction_number)

            producer.send(
                TRANSACTION_TOPIC,
                key=transaction["card_id"],
                value=transaction
            )

            producer.flush()

            print(
                f"Sent: {transaction['transaction_id']} | "
                f"Card: {transaction['card_id']} | "
                f"Amount: ₹{transaction['amount']:.2f}"
            )

            transaction_number += 1
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    start_realtime_producer()