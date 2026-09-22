from datetime import datetime


def create_high_risk_alert(transaction):

    alert = {
        "alert_id": f"ALERT-{transaction['transaction_id']}",
        "transaction_id": transaction["transaction_id"],
        "card_id": transaction["card_id"],
        "amount": transaction["amount"],
        "risk_score": transaction["risk_score"],
        "risk_level": transaction["risk_level"],
        "alert_type": "CREDIT_CARD_ANOMALY",
        "anomalies": transaction["anomalies"],
        "message": "High-risk credit card transaction detected",
        "created_at": datetime.now().isoformat()
    }

    return alert