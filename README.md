# 💳 Credit Card Anomaly Detection Using Apache Kafka

A real-time credit card transaction monitoring and anomaly detection system built using **Apache Kafka and Python**.

The system continuously receives credit card transactions through Kafka, validates incoming data, detects multiple types of anomalies, calculates a risk score, classifies transactions, routes them to appropriate Kafka topics, stores processed data in SQLite, generates JSON reports, and creates alerts for high-risk transactions.

---

## 📌 Project Overview

The **Credit Card Anomaly Detection System** demonstrates how Apache Kafka can be used to build a real-time transaction processing pipeline.

Each incoming transaction goes through the following stages:

**Transaction Generation → Kafka Producer → Validation → Anomaly Detection → Risk Scoring → Classification → Kafka Routing → Storage & Alerts**

The project also implements a **Dead-Letter Queue (DLQ)** for invalid transactions and maintains processing statistics and application logs.

---

## 🎯 Objectives

* Process credit card transactions in real time using Apache Kafka.
* Validate incoming transaction data.
* Detect different types of transaction anomalies.
* Calculate a rule-based risk score.
* Classify transactions as `NORMAL`, `SUSPICIOUS`, or `HIGH RISK`.
* Route transactions to appropriate Kafka topics.
* Generate alerts for high-risk transactions.
* Store processed transactions and alerts in SQLite.
* Generate JSON output and statistical reports.
* Handle invalid transactions using a Dead-Letter Queue.
* Maintain application logs for monitoring and debugging.

---

## 🛠️ Technologies Used

| Technology              | Purpose                   |
| ----------------------- | ------------------------- |
| **Python**              | Application development   |
| **Apache Kafka 4.3.1**  | Real-time event streaming |
| **Kafka KRaft**         | Kafka cluster management  |
| **kafka-python 3.0.11** | Python-Kafka integration  |
| **SQLite**              | Persistent data storage   |
| **JSON**                | Output and reporting      |
| **Git**                 | Version control           |
| **GitHub**              | Source code management    |
| **Windows PowerShell**  | Development environment   |

---

## 🏗️ System Architecture

```text
                    ┌──────────────────────┐
                    │ Transaction Generator│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Kafka Producer    │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌─────────────────────────────────┐
              │   credit-card-transactions      │
              └────────────────┬────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  Input Validation    │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┴─────────────┐
                 │                           │
             Invalid                       Valid
                 │                           │
                 ▼                           ▼
      ┌────────────────────┐      ┌──────────────────────┐
      │ invalid-transactions│      │ Anomaly Detection    │
      │    (DLQ Topic)      │      └──────────┬───────────┘
      └────────────────────┘                 │
                                             ▼
                                  ┌──────────────────────┐
                                  │    Risk Scoring      │
                                  └──────────┬───────────┘
                                             │
                                             ▼
                                  ┌──────────────────────┐
                                  │   Classification     │
                                  └──────────┬───────────┘
                                             │
                       ┌─────────────────────┼─────────────────────┐
                       │                     │                     │
                       ▼                     ▼                     ▼
                NORMAL                 SUSPICIOUS              HIGH RISK
                       │                     │                     │
                       ▼                     ▼                     ▼
          normal-transactions     anomaly-transactions      high-risk-alerts
                                                               │
                                                               ▼
                                                    ┌────────────────────┐
                                                    │ SQLite + JSON +    │
                                                    │ Alert Service +    │
                                                    │ Logging            │
                                                    └────────────────────┘
```

---

## 📡 Kafka Topics

The application uses the following Kafka topics:

| Topic                      | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| `credit-card-transactions` | Receives incoming credit card transactions     |
| `normal-transactions`      | Receives transactions classified as normal     |
| `anomaly-transactions`     | Receives suspicious and high-risk transactions |
| `high-risk-alerts`         | Receives high-risk transactions and alerts     |
| `invalid-transactions`     | Dead-Letter Queue for invalid transactions     |

---

# 🔍 Anomaly Detection

The system uses rule-based anomaly detection techniques to analyze each valid transaction.

## 1. 💰 Amount Anomaly

Detects unusually large transactions.

```text
Condition: Amount > ₹50,000
Risk Score: +25
```

---

## 2. 📍 Location Anomaly

Detects transactions from different locations for the same card within a short period.

```text
Location Window: 5 minutes
Risk Score: +20
```

---

## 3. ⚡ Velocity Anomaly

Detects multiple transactions from the same card within a very short period.

```text
Condition: 5 or more transactions within 60 seconds
Risk Score: +20
```

---

## 4. 🔄 Frequency Anomaly

Detects unusually frequent transactions from the same card.

```text
Condition: 10 or more transactions within 1 hour
Risk Score: +15
```

---

## 5. 🌙 Time Anomaly

Detects transactions occurring during the configured night-time period.

```text
Time Window: 00:00 – 05:00
Risk Score: +10
```

---

## 6. 🔁 Duplicate Transaction

Detects repeated transactions having the same card, amount, merchant, and location within a short period.

```text
Duplicate Window: 30 seconds
Risk Score: +25
```

---

# 📊 Risk Scoring

The system calculates a risk score based on the anomalies detected.

| Anomaly               | Risk Score |
| --------------------- | ---------: |
| Amount Anomaly        |        +25 |
| Location Anomaly      |        +20 |
| Velocity Anomaly      |        +20 |
| Frequency Anomaly     |        +15 |
| Time Anomaly          |        +10 |
| Duplicate Transaction |        +25 |

The final risk score is **capped at 100**.

> **Note:** The thresholds and scores are project-defined simulation rules intended for demonstration and learning purposes.

---

# 🚦 Transaction Classification

After anomaly detection, each transaction is classified based on its detected anomalies and risk score.

| Classification | Condition                                         |
| -------------- | ------------------------------------------------- |
| **NORMAL**     | No anomaly detected and risk score = 0            |
| **SUSPICIOUS** | At least one anomaly detected and risk score < 60 |
| **HIGH RISK**  | Risk score ≥ 60                                   |

### Classification Flow

```text
No Anomaly + Score = 0
          ↓
       NORMAL

Anomaly + Score < 60
          ↓
     SUSPICIOUS

Score ≥ 60
          ↓
     HIGH RISK
```

---

# ✅ Input Validation

Before anomaly detection, every incoming transaction passes through a validation layer.

The validation process checks:

* Transaction ID
* Card ID
* Amount
* Location
* Merchant
* Transaction type
* Timestamp
* Required fields
* Data types
* Positive transaction amount
* Valid timestamp format

Invalid transactions are **not passed to the anomaly detection engine**.

Instead, they are routed to:

```text
invalid-transactions
```

This implements a **Dead-Letter Queue (DLQ)** pattern.

---

# 🚨 High-Risk Alert System

When a transaction is classified as `HIGH RISK`, the system generates an alert.

Each alert contains:

* Alert ID
* Transaction ID
* Card ID
* Amount
* Risk score
* Risk level
* Alert type
* Detected anomalies
* Alert message
* Creation timestamp

High-risk transactions are routed to:

```text
high-risk-alerts
```

Alerts are also:

* Stored in SQLite
* Written to `output/alerts.json`
* Recorded in application logs

---

# 🗄️ SQLite Database

SQLite is used for persistent storage of processed transactions and generated alerts.

### Transactions Table

The `transactions` table stores:

* `transaction_id`
* `card_id`
* `amount`
* `location`
* `merchant`
* `transaction_type`
* `timestamp`
* `risk_score`
* `risk_level`
* `anomalies`

### Alerts Table

The `alerts` table stores:

* `alert_id`
* `transaction_id`
* `card_id`
* `amount`
* `risk_score`
* `risk_level`
* `alert_type`
* `anomalies`
* `message`
* `created_at`

Database file:

```text
credit_card_anomaly.db
```

---

# 📄 JSON Outputs

The application generates JSON files inside the `output/` directory.

### `normal_transactions.json`

Contains transactions classified as `NORMAL`.

### `anomaly_transactions.json`

Contains transactions classified as `SUSPICIOUS` or `HIGH RISK`.

### `alerts.json`

Contains generated high-risk alerts.

### `statistics.json`

Contains processing statistics such as:

* Total transactions
* Normal transactions
* Suspicious transactions
* High-risk transactions
* Invalid transactions
* Amount anomalies
* Location anomalies
* Velocity anomalies
* Frequency anomalies
* Time anomalies
* Duplicate anomalies
* Alerts generated
* Transactions saved
* Processing errors

---

# 📝 Logging

Application logs are maintained inside the `logs/` directory.

Main log file:

```text
logs/anomaly_detector.log
```

The logger records:

* Application startup
* Transaction processing
* Transaction storage
* Invalid transactions
* High-risk alerts
* Processing errors
* Application shutdown

---

# 📁 Project Structure

```text
credit-card-anomaly-detection/
│
├── producer/
│   ├── transaction_generator.py
│   └── transaction_producer.py
│
├── consumer/
│   ├── transaction_consumer.py
│   └── anomaly_detector.py
│
├── alerts/
│   ├── __init__.py
│   └── alert_service.py
│
├── storage/
│   └── database.py
│
├── config/
│   └── config.py
│
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   └── validators.py
│
├── data/
│
├── output/
│   ├── normal_transactions.json
│   ├── anomaly_transactions.json
│   ├── alerts.json
│   └── statistics.json
│
├── logs/
│   └── anomaly_detector.log
│
├── tests/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

# 🔄 End-to-End Processing Flow

```text
1. Generate Transaction
          ↓
2. Send Transaction to Kafka
          ↓
3. Consume Transaction
          ↓
4. Validate Transaction
          ↓
     ┌────┴────┐
     │         │
  Invalid    Valid
     │         │
     ▼         ▼
   DLQ     Anomaly Detection
             ↓
        Risk Calculation
             ↓
        Classification
             ↓
     ┌───────┼────────┐
     │       │        │
  NORMAL  SUSPICIOUS HIGH RISK
     │       │        │
     └───────┼────────┘
             ↓
      SQLite Storage
             +
       JSON Outputs
             +
          Logging
             +
      High-Risk Alerts
```

---

# 🧪 Testing

The project includes a `tests/` directory for validating application functionality.

Testing covers areas such as:

* Transaction validation
* Anomaly detection
* Risk scoring
* Transaction classification
* Duplicate detection
* Alert generation
* Database operations
* Invalid transaction handling

---

# 🚀 Key Features

* ⚡ Real-time Kafka transaction processing
* 🔍 Multiple anomaly detection rules
* 📊 Rule-based risk scoring
* 🚦 Transaction classification
* 🚨 Automated high-risk alerts
* ✅ Input validation
* 💀 Dead-Letter Queue for invalid transactions
* 🗄️ SQLite persistent storage
* 📄 JSON reporting
* 📈 Processing statistics
* 📝 Application logging
* 🧪 Unit testing support

---

# 🎓 Learning Outcomes

Through this project, the following concepts were implemented and practiced:

* Apache Kafka
* Kafka Producers and Consumers
* Kafka Topics
* Kafka KRaft
* Real-time event processing
* Stream-based anomaly detection
* Rule-based risk scoring
* Dead-Letter Queue pattern
* Data validation
* SQLite database integration
* JSON data processing
* Logging
* Error handling
* Python project structure
* Unit testing
* Git and GitHub workflow

---

# ⚠️ Disclaimer

This project is an **educational simulation** of a real-time credit card anomaly detection system.

The anomaly thresholds, risk scores, and classification rules are project-defined demonstration rules and should not be considered production financial fraud-detection criteria.

---


## ⭐ Project Summary

This project demonstrates a complete real-time event-processing pipeline using **Apache Kafka and Python**, combining streaming, validation, anomaly detection, risk scoring, alert generation, persistent storage, JSON reporting, logging, and Dead-Letter Queue processing into a single credit card monitoring application.
