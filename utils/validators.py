from datetime import datetime


REQUIRED_FIELDS = [
    "transaction_id",
    "card_id",
    "amount",
    "location",
    "merchant",
    "transaction_type",
    "timestamp"
]


def validate_transaction(transaction):

    # Check whether the transaction is a dictionary
    if not isinstance(transaction, dict):
        return False, "Transaction is not a JSON object"

    # Check required fields
    for field in REQUIRED_FIELDS:

        if field not in transaction:
            return False, f"Missing field: {field}"

    # Validate transaction ID
    if (
        not isinstance(transaction["transaction_id"], str)
        or not transaction["transaction_id"].strip()
    ):
        return False, "Invalid transaction_id"

    # Validate card ID
    if (
        not isinstance(transaction["card_id"], str)
        or not transaction["card_id"].strip()
    ):
        return False, "Invalid card_id"

    # Validate amount
    try:
        amount = float(transaction["amount"])

        if amount <= 0:
            return False, "Amount must be greater than zero"

    except (ValueError, TypeError):
        return False, "Amount must be numeric"

    # Validate location
    if (
        not isinstance(transaction["location"], str)
        or not transaction["location"].strip()
    ):
        return False, "Invalid location"

    # Validate merchant
    if (
        not isinstance(transaction["merchant"], str)
        or not transaction["merchant"].strip()
    ):
        return False, "Invalid merchant"

    # Validate transaction type
    if (
        not isinstance(transaction["transaction_type"], str)
        or not transaction["transaction_type"].strip()
    ):
        return False, "Invalid transaction_type"

    # Validate timestamp
    try:
        datetime.fromisoformat(
            transaction["timestamp"]
        )

    except Exception:
        return False, "Invalid timestamp"

    return True, "Valid transaction"