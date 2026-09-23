import os
import json
import sqlite3
import pandas as pd
import streamlit as st


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Credit Card Anomaly Detection",
    page_icon="💳",
    layout="wide"
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = os.path.dirname(
    os.path.abspath(__file__)
)

DATABASE_PATH = os.path.join(
    PROJECT_ROOT,
    "credit_card_anomaly.db"
)

STATISTICS_PATH = os.path.join(
    PROJECT_ROOT,
    "output",
    "statistics.json"
)

ALERTS_PATH = os.path.join(
    PROJECT_ROOT,
    "output",
    "alerts.json"
)


# ============================================================
# DATABASE FUNCTIONS
# ============================================================

def get_connection():
    return sqlite3.connect(
        DATABASE_PATH
    )


def load_transactions():

    connection = get_connection()

    query = """
        SELECT
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
        FROM transactions
        ORDER BY rowid DESC
    """

    dataframe = pd.read_sql_query(
        query,
        connection
    )

    connection.close()

    return dataframe


def load_alerts():

    connection = get_connection()

    query = """
        SELECT
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
        FROM alerts
        ORDER BY rowid DESC
    """

    dataframe = pd.read_sql_query(
        query,
        connection
    )

    connection.close()

    return dataframe


# ============================================================
# LOAD STATISTICS
# ============================================================

def load_statistics():

    if not os.path.exists(
        STATISTICS_PATH
    ):

        return {}

    try:

        with open(
            STATISTICS_PATH,
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    except Exception:

        return {}


# ============================================================
# PAGE HEADER
# ============================================================

st.title(
    "💳 Credit Card Anomaly Detection"
)

st.subheader(
    "Real-Time Monitoring Dashboard"
)

st.caption(
    "Apache Kafka + Python + SQLite + Streamlit"
)


# ============================================================
# REFRESH BUTTON
# ============================================================

if st.button(
    "🔄 Refresh Dashboard"
):

    st.rerun()


# ============================================================
# LOAD DATA
# ============================================================

statistics = load_statistics()

try:

    transactions = load_transactions()

except Exception as error:

    st.error(
        f"Unable to load SQLite database: {error}"
    )

    st.stop()


try:

    alerts = load_alerts()

except Exception:

    alerts = pd.DataFrame()


# ============================================================
# KPI METRICS
# ============================================================

st.markdown(
    "## 📊 Transaction Overview"
)

col1, col2, col3, col4, col5 = st.columns(5)


total_transactions = statistics.get(
    "total_transactions",
    len(transactions)
)

normal_transactions = statistics.get(
    "normal_transactions",
    0
)

suspicious_transactions = statistics.get(
    "suspicious_transactions",
    0
)

high_risk_transactions = statistics.get(
    "high_risk_transactions",
    0
)

invalid_transactions = statistics.get(
    "invalid_transactions",
    0
)


with col1:

    st.metric(
        "Total Transactions",
        total_transactions
    )


with col2:

    st.metric(
        "🟢 Normal",
        normal_transactions
    )


with col3:

    st.metric(
        "🟠 Suspicious",
        suspicious_transactions
    )


with col4:

    st.metric(
        "🔴 High Risk",
        high_risk_transactions
    )


with col5:

    st.metric(
        "❌ Invalid",
        invalid_transactions
    )


# ============================================================
# ALERT SUMMARY
# ============================================================

st.markdown(
    "## 🚨 Alert Summary"
)

alert_col1, alert_col2, alert_col3 = st.columns(3)


with alert_col1:

    st.metric(
        "Alerts Generated",
        statistics.get(
            "alerts_generated",
            len(alerts)
        )
    )


with alert_col2:

    st.metric(
        "Transactions Saved",
        statistics.get(
            "transactions_saved",
            len(transactions)
        )
    )


with alert_col3:

    st.metric(
        "Processing Errors",
        statistics.get(
            "processing_errors",
            0
        )
    )


# ============================================================
# RISK DISTRIBUTION
# ============================================================

st.markdown(
    "## 📈 Risk Distribution"
)

risk_data = pd.DataFrame(
    {
        "Risk Level": [
            "NORMAL",
            "SUSPICIOUS",
            "HIGH RISK"
        ],
        "Count": [
            normal_transactions,
            suspicious_transactions,
            high_risk_transactions
        ]
    }
)

chart_col1, chart_col2 = st.columns(2)


with chart_col1:

    st.bar_chart(
        risk_data.set_index(
            "Risk Level"
        )
    )


with chart_col2:

    st.dataframe(
        risk_data,
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# ANOMALY STATISTICS
# ============================================================

st.markdown(
    "## 🔍 Anomaly Statistics"
)

anomaly_data = pd.DataFrame(
    {
        "Anomaly Type": [
            "Amount",
            "Location",
            "Velocity",
            "Frequency",
            "Time",
            "Duplicate"
        ],
        "Count": [
            statistics.get(
                "amount_anomalies",
                0
            ),
            statistics.get(
                "location_anomalies",
                0
            ),
            statistics.get(
                "velocity_anomalies",
                0
            ),
            statistics.get(
                "frequency_anomalies",
                0
            ),
            statistics.get(
                "time_anomalies",
                0
            ),
            statistics.get(
                "duplicate_anomalies",
                0
            )
        ]
    }
)

st.bar_chart(
    anomaly_data.set_index(
        "Anomaly Type"
    )
)


# ============================================================
# HIGH-RISK ALERTS
# ============================================================

st.markdown(
    "## 🚨 High-Risk Alerts"
)

if not alerts.empty:

    st.dataframe(
        alerts,
        use_container_width=True,
        hide_index=True
    )

else:

    st.info(
        "No high-risk alerts available."
    )


# ============================================================
# RECENT TRANSACTIONS
# ============================================================

st.markdown(
    "## 📋 Recent Transactions"
)

if not transactions.empty:

    recent_transactions = transactions.head(
        20
    )

    st.dataframe(
        recent_transactions,
        use_container_width=True,
        hide_index=True
    )

else:

    st.info(
        "No transactions available."
    )


# ============================================================
# TRANSACTION FILTER
# ============================================================

st.markdown(
    "## 🔎 Filter Transactions"
)

if not transactions.empty:

    selected_risk = st.selectbox(
        "Select Risk Level",
        [
            "ALL",
            "NORMAL",
            "SUSPICIOUS",
            "HIGH RISK"
        ]
    )

    if selected_risk == "ALL":

        filtered_transactions = (
            transactions
        )

    else:

        filtered_transactions = (
            transactions[
                transactions[
                    "risk_level"
                ]
                ==
                selected_risk
            ]
        )

    st.dataframe(
        filtered_transactions,
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")

st.caption(
    "Credit Card Anomaly Detection Using Apache Kafka"
)