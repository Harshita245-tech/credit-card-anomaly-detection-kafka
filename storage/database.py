import sqlite3


DATABASE_NAME = "credit_card_anomaly.db"


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    return sqlite3.connect(DATABASE_NAME)


# ============================================================
# CREATE DATABASE AND TABLES
# ============================================================

def create_database():

    connection = get_connection()
    cursor = connection.cursor()

    # ========================================================
    # TRANSACTIONS TABLE
    # ========================================================

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (

            transaction_id TEXT PRIMARY KEY,

            card_id TEXT,

            amount REAL,

            location TEXT,

            merchant TEXT,

            transaction_type TEXT,

            timestamp TEXT,

            risk_score INTEGER,

            risk_level TEXT,

            anomalies TEXT
        )
    """)

    # ========================================================
    # ALERTS TABLE
    # ========================================================

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (

            alert_id TEXT PRIMARY KEY,

            transaction_id TEXT,

            card_id TEXT,

            amount REAL,

            risk_score INTEGER,

            risk_level TEXT,

            alert_type TEXT,

            anomalies TEXT,

            message TEXT,

            created_at TEXT
        )
    """)

    connection.commit()
    connection.close()


# ============================================================
# SAVE TRANSACTION
# ============================================================

def save_transaction(transaction):

    connection = get_connection()
    cursor = connection.cursor()

    anomalies = ", ".join(
        transaction["anomalies"]
    )

    cursor.execute("""
        INSERT OR REPLACE INTO transactions (

            transaction_id,
            card_id,
            amount,
            location,
            merchant,
            transaction_type,
            timestamp,
            risk_score,
            risk_level,
            anomalies

        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (

        transaction["transaction_id"],
        transaction["card_id"],
        transaction["amount"],
        transaction["location"],
        transaction["merchant"],
        transaction["transaction_type"],
        transaction["timestamp"],
        transaction["risk_score"],
        transaction["risk_level"],
        anomalies

    ))

    connection.commit()
    connection.close()


# ============================================================
# SAVE HIGH-RISK ALERT
# ============================================================

def save_alert(alert):

    connection = get_connection()
    cursor = connection.cursor()

    anomalies = ", ".join(
        alert["anomalies"]
    )

    cursor.execute("""
        INSERT OR REPLACE INTO alerts (

            alert_id,
            transaction_id,
            card_id,
            amount,
            risk_score,
            risk_level,
            alert_type,
            anomalies,
            message,
            created_at

        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (

        alert["alert_id"],
        alert["transaction_id"],
        alert["card_id"],
        alert["amount"],
        alert["risk_score"],
        alert["risk_level"],
        alert["alert_type"],
        anomalies,
        alert["message"],
        alert["created_at"]

    ))

    connection.commit()
    connection.close()


# ============================================================
# TRANSACTION HISTORY
# ============================================================

def get_transaction_history(limit=20):

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            transaction_id,
            card_id,
            amount,
            location,
            merchant,
            timestamp,
            risk_score,
            risk_level,
            anomalies

        FROM transactions

        ORDER BY rowid DESC

        LIMIT ?
    """, (limit,))

    records = cursor.fetchall()

    connection.close()

    return records


# ============================================================
# HIGH-RISK ALERT HISTORY
# ============================================================

def get_high_risk_alerts(limit=20):

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            alert_id,
            transaction_id,
            card_id,
            amount,
            risk_score,
            anomalies,
            created_at

        FROM alerts

        ORDER BY rowid DESC

        LIMIT ?
    """, (limit,))

    records = cursor.fetchall()

    connection.close()

    return records


# ============================================================
# DIRECT TEST
# ============================================================

if __name__ == "__main__":

    create_database()

    print(
        "SQLite database created successfully."
    )

    print(
        f"Database: {DATABASE_NAME}"
    )