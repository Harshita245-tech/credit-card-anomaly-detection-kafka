\# Credit Card Anomaly Detection Using Apache Kafka



\## 1. Project Overview



Credit Card Anomaly Detection Using Apache Kafka is a real-time transaction monitoring system developed using Apache Kafka and Python.



The system continuously receives credit card transactions through Kafka, validates each transaction, analyzes it for different types of anomalies, calculates a risk score, classifies the transaction, and routes it to the appropriate Kafka topic.



The system also stores processed transactions and high-risk alerts in SQLite, generates JSON output files, maintains processing statistics, and records application logs.



\---



\## 2. Objectives



The main objectives of this project are:



\- To process credit card transactions in real time using Apache Kafka.

\- To detect different types of transaction anomalies.

\- To calculate a risk score for every transaction.

\- To classify transactions as NORMAL, SUSPICIOUS, or HIGH RISK.

\- To route transactions to appropriate Kafka topics.

\- To generate alerts for high-risk transactions.

\- To validate incoming transaction data.

\- To handle invalid transactions using a dead-letter topic.

\- To store transaction and alert information using SQLite.

\- To generate JSON outputs and processing statistics.

\- To maintain application logs for monitoring and debugging.



\---



\## 3. Technologies Used



\- Python

\- Apache Kafka 4.3.1

\- Kafka KRaft

\- kafka-python 3.0.11

\- SQLite

\- JSON

\- Git

\- GitHub

\- Windows PowerShell



\---



\## 4. System Architecture



The overall flow of the project is:



Transaction Generator  

↓  

Kafka Producer  

↓  

`credit-card-transactions`  

↓  

Anomaly Detection Engine  

↓  

Validation and Anomaly Detection  

↓  

Risk Scoring and Classification  

↓  

Kafka Topic Routing  

↓  

SQLite + JSON Output + Logging + Alerts



Invalid transactions are routed separately:



Invalid Transaction  

↓  

Validation  

↓  

`invalid-transactions`  

↓  

Dead-Letter Queue



\---



\## 5. Kafka Topics



The project uses the following Kafka topics:



| Kafka Topic | Purpose |

|---|---|

| `credit-card-transactions` | Receives incoming credit card transactions |

| `normal-transactions` | Stores/routs transactions classified as normal |

| `anomaly-transactions` | Stores/routs suspicious transactions |

| `high-risk-alerts` | Receives high-risk transactions and alerts |

| `invalid-transactions` | Dead-letter topic for invalid transactions |



\---



\## 6. Anomaly Detection



The anomaly detection engine checks every valid transaction using multiple rule-based detection techniques.



\### 6.1 Amount Anomaly



A transaction is considered an amount anomaly when:



`Amount > ₹50,000`



Risk score:



`+25`



\---



\### 6.2 Location Anomaly



A location anomaly is detected when the same card performs transactions from different locations within a short period.



Current location window:



`5 minutes`



Risk score:



`+20`



\---



\### 6.3 Velocity Anomaly



Velocity anomaly detects multiple transactions from the same card within a short time.



Current rule:



`5 or more transactions within 60 seconds`



Risk score:



`+20`



\---



\### 6.4 Frequency Anomaly



Frequency anomaly detects unusually frequent transactions from the same card.



Current rule:



`10 or more transactions within 1 hour`



Risk score:



`+15`



\---



\### 6.5 Time Anomaly



Time anomaly detects transactions occurring during the defined night-time period.



Current time window:



`00:00 - 05:00`



Risk score:



`+10`



\---



\### 6.6 Duplicate Transaction



Duplicate detection checks whether the same card performs a transaction with the same amount, merchant, and location within a short period.



Current duplicate window:



`30 seconds`



Risk score:



`+25`



\---



\## 7. Risk Scoring



The project uses a rule-based risk scoring mechanism.



| Anomaly Type | Risk Score |

|---|---:|

| Amount Anomaly | +25 |

| Location Anomaly | +20 |

| Velocity Anomaly | +20 |

| Frequency Anomaly | +15 |

| Time Anomaly | +10 |

| Duplicate Anomaly | +25 |



The final risk score is capped at 100.



These thresholds and scores are project-defined simulation rules used for this demonstration project.



\---



\## 8. Transaction Classification



Transactions are classified based on detected anomalies and the calculated risk score.



\### NORMAL



A transaction is classified as NORMAL when:



\- No anomaly is detected.

\- Risk score is 0.



\### SUSPICIOUS



A transaction is classified as SUSPICIOUS when:



\- At least one anomaly is detected.

\- Risk score is below 60.



\### HIGH RISK



A transaction is classified as HIGH RISK when:



\- Risk score is 60 or above.



The classification flow is:



No Anomaly + Score 0  

→ NORMAL



Anomaly + Score < 60  

→ SUSPICIOUS



Score >= 60  

→ HIGH RISK



\---



\## 9. Input Validation



Before anomaly detection, incoming transactions are validated.



The validation checks:



\- Transaction ID

\- Card ID

\- Amount

\- Location

\- Merchant

\- Transaction type

\- Timestamp

\- Required fields

\- Valid data types

\- Positive transaction amount

\- Valid timestamp format



If a transaction is invalid, it is not processed by the anomaly detection logic.



Instead, it is routed to:



`invalid-transactions`



This implements a dead-letter queue pattern.



\---



\## 10. High-Risk Alert System



When a transaction is classified as HIGH RISK, the system creates a high-risk alert.



The alert contains:



\- Alert ID

\- Transaction ID

\- Card ID

\- Amount

\- Risk score

\- Risk level

\- Alert type

\- Detected anomalies

\- Alert message

\- Creation timestamp



High-risk transactions are routed to:



`high-risk-alerts`



Alerts are also stored in SQLite and written to:



`output/alerts.json`



\---



\## 11. SQLite Database



SQLite is used for persistent storage.



\### Transactions Table



The `transactions` table stores:



\- transaction\_id

\- card\_id

\- amount

\- location

\- merchant

\- transaction\_type

\- timestamp

\- risk\_score

\- risk\_level

\- anomalies



\### Alerts Table



The `alerts` table stores:



\- alert\_id

\- transaction\_id

\- card\_id

\- amount

\- risk\_score

\- risk\_level

\- alert\_type

\- anomalies

\- message

\- created\_at



The database file is:



`credit\_card\_anomaly.db`



\---



\## 12. JSON Output



The system generates JSON output files inside the `output` directory.



\### normal\_transactions.json



Contains transactions classified as NORMAL.



\### anomaly\_transactions.json



Contains transactions classified as SUSPICIOUS or HIGH RISK.



\### alerts.json



Contains generated high-risk alerts.



\### statistics.json



Contains processing statistics such as:



\- Total transactions

\- Normal transactions

\- Suspicious transactions

\- High-risk transactions

\- Invalid transactions

\- Amount anomalies

\- Location anomalies

\- Velocity anomalies

\- Frequency anomalies

\- Time anomalies

\- Duplicate anomalies

\- Alerts generated

\- Transactions saved

\- Processing errors



\---



\## 13. Logging



The project maintains application logs inside the `logs` directory.



Main log file:



`logs/anomaly\_detector.log`



The logger records:



\- Application startup

\- Transaction processing

\- Transaction storage

\- Invalid transactions

\- High-risk alerts

\- Processing errors

\- Application shutdown



\---



\## 14. Project Structure



```text

credit-card-anomaly-detection/

│

├── producer/

│   ├── transaction\_generator.py

│   └── transaction\_producer.py

│

├── consumer/

│   ├── transaction\_consumer.py

│   └── anomaly\_detector.py

│

├── alerts/

│   ├── \_\_init\_\_.py

│   └── alert\_service.py

│

├── storage/

│   └── database.py

│

├── config/

│   └── config.py

│

├── utils/

│   ├── \_\_init\_\_.py

│   ├── logger.py

│   └── validators.py

│

├── data/

│

├── output/

│   ├── normal\_transactions.json

│   ├── anomaly\_transactions.json

│   ├── alerts.json

│   └── statistics.json

│

├── logs/

│   └── anomaly\_detector.log

│

├── tests/

│

├── requirements.txt

├── README.md

└── .gitignore

