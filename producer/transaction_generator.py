import random
from datetime import datetime


def generate_transaction(transaction_number):
    transaction = {
        "transaction_id": f"TXN{transaction_number:05d}",
        "card_id": random.choice(
            ["CARD1001", "CARD1002", "CARD1003", "CARD1004", "CARD1005"]
        ),
        "amount": round(random.uniform(100, 10000), 2),
        "location": random.choice(
            ["Hyderabad", "Bangalore", "Chennai", "Mumbai", "Delhi"]
        ),
        "merchant": random.choice(
            ["Amazon", "Walmart", "Flipkart", "Uber", "Restaurant"]
        ),
        "transaction_type": "PURCHASE",
        "timestamp": datetime.now().isoformat()
    }

    return transaction