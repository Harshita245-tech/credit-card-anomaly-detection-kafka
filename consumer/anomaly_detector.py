import sys
import os
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque

# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

sys.path.append(PROJECT_ROOT)


# ============================================================
# KAFKA
# ============================================================

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


# ============================================================
# CONFIGURATION
# ============================================================

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TRANSACTION_TOPIC,
    NORMAL_TOPIC,
    ANOMALY_TOPIC,
    HIGH_RISK_TOPIC,
    INVALID_TOPIC
)


# ============================================================
# PROJECT MODULES
# ============================================================

from storage.database import (
    create_database,
    save_transaction,
    save_alert
)

from utils.validators import validate_transaction

from utils.logger import setup_logger

from alerts.alert_service import create_high_risk_alert


# ============================================================
# DIRECTORIES
# ============================================================

OUTPUT_DIR = os.path.join(
    PROJECT_ROOT,
    "output"
)

LOG_DIR = os.path.join(
    PROJECT_ROOT,
    "logs"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)

os.makedirs(
    LOG_DIR,
    exist_ok=True
)


# ============================================================
# APPLICATION LOGGER
# ============================================================

logger = setup_logger(
    "anomaly_detector",
    os.path.join(
        LOG_DIR,
        "anomaly_detector.log"
    )
)


# ============================================================
# CONFIGURATION-BASED THRESHOLDS
# ============================================================

AMOUNT_THRESHOLD = 50000

VELOCITY_WINDOW_SECONDS = 60
VELOCITY_TRANSACTION_LIMIT = 5

FREQUENCY_WINDOW_SECONDS = 60 * 60
FREQUENCY_TRANSACTION_LIMIT = 10

LOCATION_WINDOW_SECONDS = 5 * 60

DUPLICATE_WINDOW_SECONDS = 30

NIGHT_START_HOUR = 0
NIGHT_END_HOUR = 5


# ============================================================
# RISK SCORES
# ============================================================

AMOUNT_SCORE = 25
LOCATION_SCORE = 20
VELOCITY_SCORE = 20
FREQUENCY_SCORE = 15
TIME_SCORE = 10
DUPLICATE_SCORE = 25


# ============================================================
# STREAM PROCESSING MEMORY
# ============================================================

last_transaction_by_card = {}

card_transaction_times = defaultdict(
    deque
)

card_frequency_times = defaultdict(
    deque
)

recent_transactions = defaultdict(
    deque
)


# ============================================================
# PROCESSING STATISTICS
# ============================================================

statistics = {

    "total_transactions": 0,

    "normal_transactions": 0,

    "suspicious_transactions": 0,

    "high_risk_transactions": 0,

    "invalid_transactions": 0,

    "amount_anomalies": 0,

    "location_anomalies": 0,

    "velocity_anomalies": 0,

    "frequency_anomalies": 0,

    "time_anomalies": 0,

    "duplicate_anomalies": 0,

    "alerts_generated": 0,

    "transactions_saved": 0,

    "processing_errors": 0
}


# ============================================================
# OUTPUT FILES
# ============================================================

NORMAL_JSON = os.path.join(
    OUTPUT_DIR,
    "normal_transactions.json"
)

ANOMALY_JSON = os.path.join(
    OUTPUT_DIR,
    "anomaly_transactions.json"
)

ALERT_JSON = os.path.join(
    OUTPUT_DIR,
    "alerts.json"
)

STATISTICS_JSON = os.path.join(
    OUTPUT_DIR,
    "statistics.json"
)


# ============================================================
# JSON OUTPUT FUNCTION
# ============================================================

def append_json_record(
    filename,
    record
):

    try:

        data = []

        if os.path.exists(filename):

            with open(
                filename,
                "r",
                encoding="utf-8"
            ) as file:

                try:

                    data = json.load(file)

                    if not isinstance(
                        data,
                        list
                    ):

                        data = []

                except json.JSONDecodeError:

                    data = []

        data.append(record)

        with open(
            filename,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=4,
                ensure_ascii=False
            )

    except Exception as error:

        logger.error(
            f"JSON output error: {error}"
        )


# ============================================================
# SAVE STATISTICS
# ============================================================

def save_statistics():

    try:

        with open(
            STATISTICS_JSON,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                statistics,
                file,
                indent=4
            )

    except Exception as error:

        logger.error(
            f"Statistics save error: {error}"
        )


# ============================================================
# TIMESTAMP PARSER
# ============================================================

def parse_timestamp(timestamp):

    try:

        return datetime.fromisoformat(
            timestamp
        )

    except Exception:

        return datetime.now()


# ============================================================
# CREATE DATABASE
# ============================================================

create_database()

logger.info(
    "Credit Card Anomaly Detection Engine started"
)


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(

    TRANSACTION_TOPIC,

    bootstrap_servers=
        KAFKA_BOOTSTRAP_SERVERS,

    group_id=
        "anomaly-detection-engine-v3",

    auto_offset_reset=
        "latest",

    enable_auto_commit=
        True,

    key_deserializer=
        lambda key:
            key.decode("utf-8")
            if key
            else None,

    value_deserializer=
        lambda value:
            json.loads(
                value.decode("utf-8")
            )
)


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(

    bootstrap_servers=
        KAFKA_BOOTSTRAP_SERVERS,

    key_serializer=
        lambda key:
            key.encode("utf-8"),

    value_serializer=
        lambda value:
            json.dumps(
                value
            ).encode("utf-8")
)


# ============================================================
# STARTUP MESSAGE
# ============================================================

print()

print(
    "========================================"
)

print(
    "     CREDIT CARD ANOMALY DETECTOR"
)

print(
    "========================================"
)

print()

print(
    "Kafka connected successfully."
)

print(
    f"Listening to : {TRANSACTION_TOPIC}"
)

print()

print(
    "Detection Rules:"
)

print(
    f"Amount threshold      : "
    f"₹{AMOUNT_THRESHOLD}"
)

print(
    f"Velocity limit        : "
    f"{VELOCITY_TRANSACTION_LIMIT} "
    f"transactions / "
    f"{VELOCITY_WINDOW_SECONDS} seconds"
)

print(
    f"Frequency limit       : "
    f"{FREQUENCY_TRANSACTION_LIMIT} "
    f"transactions / hour"
)

print(
    f"Location window       : "
    f"{LOCATION_WINDOW_SECONDS} seconds"
)

print(
    f"Duplicate window      : "
    f"{DUPLICATE_WINDOW_SECONDS} seconds"
)

print()

print(
    "Kafka routing enabled."
)

print(
    "Input validation enabled."
)

print(
    "SQLite storage enabled."
)

print(
    "Alert database enabled."
)

print(
    "JSON output enabled."
)

print(
    "Statistics enabled."
)

print(
    "Application logging enabled."
)

print()

print(
    "Waiting for transactions..."
)

print()


# ============================================================
# MAIN PROCESSING LOOP
# ============================================================

try:

    for message in consumer:

        try:

            # =================================================
            # READ TRANSACTION
            # =================================================

            transaction = message.value


            # =================================================
            # VALIDATION
            # =================================================

            is_valid, validation_error = (
                validate_transaction(
                    transaction
                )
            )


            # =================================================
            # INVALID TRANSACTION / DLQ
            # =================================================

            if not is_valid:

                statistics[
                    "invalid_transactions"
                ] += 1

                transaction_id = (
                    transaction.get(
                        "transaction_id",
                        "UNKNOWN"
                    )
                    if isinstance(
                        transaction,
                        dict
                    )
                    else "UNKNOWN"
                )

                print()

                print(
                    "========================================"
                )

                print(
                    f"❌ INVALID TRANSACTION | "
                    f"{transaction_id}"
                )

                print(
                    f"Reason: "
                    f"{validation_error}"
                )

                print(
                    "📤 ROUTED → "
                    "invalid-transactions"
                )

                print(
                    "========================================"
                )

                print()

                logger.warning(
                    f"Invalid transaction: "
                    f"{transaction_id} | "
                    f"{validation_error}"
                )

                try:

                    producer.send(

                        INVALID_TOPIC,

                        key=transaction_id,

                        value={

                            "transaction":
                                transaction,

                            "error":
                                validation_error,

                            "timestamp":
                                datetime.now().isoformat()
                        }
                    )

                    producer.flush()

                except KafkaError as error:

                    logger.error(
                        f"DLQ routing error: "
                        f"{error}"
                    )

                save_statistics()

                continue


            # =================================================
            # EXTRACT DATA
            # =================================================

            transaction_id = (
                transaction[
                    "transaction_id"
                ]
            )

            card_id = (
                transaction[
                    "card_id"
                ]
            )

            amount = float(
                transaction[
                    "amount"
                ]
            )

            location = (
                transaction[
                    "location"
                ]
            )

            merchant = (
                transaction[
                    "merchant"
                ]
            )

            transaction_type = (
                transaction[
                    "transaction_type"
                ]
            )

            timestamp = (
                transaction[
                    "timestamp"
                ]
            )

            current_time = (
                parse_timestamp(
                    timestamp
                )
            )


            # =================================================
            # COUNTER
            # =================================================

            statistics[
                "total_transactions"
            ] += 1


            # =================================================
            # PROCESSING HEADER
            # =================================================

            print()

            print(
                "========================================"
            )

            print(
                f"Processing: "
                f"{transaction_id} | "
                f"Card: {card_id} | "
                f"Amount: ₹{amount:.2f}"
            )


            logger.info(
                f"Processing: "
                f"{transaction_id} | "
                f"Card: {card_id} | "
                f"Amount: ₹{amount:.2f}"
            )


            # =================================================
            # VARIABLES
            # =================================================

            anomalies = []

            risk_score = 0


            # =================================================
            # 1. AMOUNT ANOMALY
            # =================================================

            if amount > AMOUNT_THRESHOLD:

                print(
                    f"⚠️ AMOUNT ANOMALY | "
                    f"₹{amount:.2f} > "
                    f"₹{AMOUNT_THRESHOLD}"
                )

                anomalies.append(
                    "Amount Anomaly"
                )

                risk_score += (
                    AMOUNT_SCORE
                )

                statistics[
                    "amount_anomalies"
                ] += 1

            else:

                print(
                    f"✓ Amount normal | "
                    f"₹{amount:.2f}"
                )


            # =================================================
            # 2. LOCATION ANOMALY
            # =================================================

            location_anomaly = False

            if card_id in (
                last_transaction_by_card
            ):

                previous = (
                    last_transaction_by_card[
                        card_id
                    ]
                )

                previous_location = (
                    previous[
                        "location"
                    ]
                )

                previous_time = (
                    previous[
                        "time"
                    ]
                )

                time_difference = (
                    current_time -
                    previous_time
                ).total_seconds()


                if (
                    previous_location
                    != location
                    and
                    time_difference
                    <= LOCATION_WINDOW_SECONDS
                ):

                    location_anomaly = True

                    print(
                        f"⚠️ LOCATION ANOMALY | "
                        f"{previous_location} → "
                        f"{location} within "
                        f"{int(time_difference)} seconds"
                    )

                    anomalies.append(
                        "Location Anomaly"
                    )

                    risk_score += (
                        LOCATION_SCORE
                    )

                    statistics[
                        "location_anomalies"
                    ] += 1


            if not location_anomaly:

                print(
                    f"✓ Location normal | "
                    f"{location}"
                )


            # =================================================
            # 3. VELOCITY ANOMALY
            # =================================================

            velocity_times = (
                card_transaction_times[
                    card_id
                ]
            )

            cutoff_time = (
                current_time -
                timedelta(
                    seconds=
                    VELOCITY_WINDOW_SECONDS
                )
            )


            while (
                velocity_times
                and
                velocity_times[0]
                < cutoff_time
            ):

                velocity_times.popleft()


            velocity_count = (
                len(
                    velocity_times
                ) + 1
            )


            velocity_anomaly = (
                velocity_count
                >= VELOCITY_TRANSACTION_LIMIT
            )


            if velocity_anomaly:

                print(
                    f"⚠️ VELOCITY ANOMALY | "
                    f"{velocity_count} "
                    f"transactions within "
                    f"{VELOCITY_WINDOW_SECONDS} seconds"
                )

                anomalies.append(
                    "Velocity Anomaly"
                )

                risk_score += (
                    VELOCITY_SCORE
                )

                statistics[
                    "velocity_anomalies"
                ] += 1

            else:

                print(
                    f"✓ Velocity normal | "
                    f"{velocity_count} "
                    f"transaction(s) in last minute"
                )


            # =================================================
            # 4. FREQUENCY ANOMALY
            # =================================================

            frequency_times = (
                card_frequency_times[
                    card_id
                ]
            )

            frequency_cutoff = (
                current_time -
                timedelta(
                    seconds=
                    FREQUENCY_WINDOW_SECONDS
                )
            )


            while (
                frequency_times
                and
                frequency_times[0]
                < frequency_cutoff
            ):

                frequency_times.popleft()


            frequency_count = (
                len(
                    frequency_times
                ) + 1
            )


            frequency_anomaly = (
                frequency_count
                >= FREQUENCY_TRANSACTION_LIMIT
            )


            if frequency_anomaly:

                print(
                    f"⚠️ FREQUENCY ANOMALY | "
                    f"{frequency_count} "
                    f"transactions in last hour"
                )

                anomalies.append(
                    "Frequency Anomaly"
                )

                risk_score += (
                    FREQUENCY_SCORE
                )

                statistics[
                    "frequency_anomalies"
                ] += 1

            else:

                print(
                    f"✓ Frequency normal | "
                    f"{frequency_count} "
                    f"transaction(s) in last hour"
                )


            # =================================================
            # 5. TIME ANOMALY
            # =================================================

            hour = (
                current_time.hour
            )


            if (
                NIGHT_START_HOUR
                <= hour
                <
                NIGHT_END_HOUR
            ):

                print(
                    f"⚠️ TIME ANOMALY | "
                    f"Transaction at "
                    f"{current_time.strftime('%H:%M:%S')}"
                )

                anomalies.append(
                    "Time Anomaly"
                )

                risk_score += (
                    TIME_SCORE
                )

                statistics[
                    "time_anomalies"
                ] += 1

            else:

                print(
                    f"✓ Transaction time normal | "
                    f"{current_time.strftime('%H:%M:%S')}"
                )


            # =================================================
            # 6. DUPLICATE TRANSACTION
            # =================================================

            duplicate_anomaly = False

            transaction_history = (
                recent_transactions[
                    card_id
                ]
            )

            duplicate_cutoff = (
                current_time -
                timedelta(
                    seconds=
                    DUPLICATE_WINDOW_SECONDS
                )
            )


            while (
                transaction_history
                and
                transaction_history[0][
                    "time"
                ]
                < duplicate_cutoff
            ):

                transaction_history.popleft()


            for previous_transaction in (
                transaction_history
            ):

                if (
                    previous_transaction[
                        "amount"
                    ]
                    == amount
                    and
                    previous_transaction[
                        "merchant"
                    ]
                    == merchant
                    and
                    previous_transaction[
                        "location"
                    ]
                    == location
                ):

                    duplicate_anomaly = True

                    break


            if duplicate_anomaly:

                print(
                    "⚠️ DUPLICATE TRANSACTION"
                )

                anomalies.append(
                    "Duplicate Anomaly"
                )

                risk_score += (
                    DUPLICATE_SCORE
                )

                statistics[
                    "duplicate_anomalies"
                ] += 1

            else:

                print(
                    "✓ Duplicate check normal"
                )


            # =================================================
            # UPDATE STREAM MEMORY
            # =================================================

            last_transaction_by_card[
                card_id
            ] = {

                "location":
                    location,

                "time":
                    current_time
            }


            velocity_times.append(
                current_time
            )

            frequency_times.append(
                current_time
            )

            transaction_history.append(

                {

                    "transaction_id":
                        transaction_id,

                    "amount":
                        amount,

                    "merchant":
                        merchant,

                    "location":
                        location,

                    "time":
                        current_time
                }
            )


            # =================================================
            # CAP RISK SCORE
            # =================================================

            risk_score = min(
                risk_score,
                100
            )


            # =================================================
            # CLASSIFICATION
            # =================================================

            if risk_score >= 60:

                risk_level = (
                    "HIGH RISK"
                )

            elif anomalies:

                risk_level = (
                    "SUSPICIOUS"
                )

            else:

                risk_level = (
                    "NORMAL"
                )


            # =================================================
            # PROCESSED TRANSACTION
            # =================================================

            processed_transaction = {

                "transaction_id":
                    transaction_id,

                "card_id":
                    card_id,

                "amount":
                    amount,

                "location":
                    location,

                "merchant":
                    merchant,

                "transaction_type":
                    transaction_type,

                "timestamp":
                    timestamp,

                "risk_score":
                    risk_score,

                "risk_level":
                    risk_level,

                "anomalies":
                    anomalies
            }


            # =================================================
            # CLASSIFICATION COUNTERS
            # =================================================

            if risk_level == "NORMAL":

                statistics[
                    "normal_transactions"
                ] += 1


            elif risk_level == "SUSPICIOUS":

                statistics[
                    "suspicious_transactions"
                ] += 1


            elif risk_level == "HIGH RISK":

                statistics[
                    "high_risk_transactions"
                ] += 1


            # =================================================
            # SAVE TRANSACTION TO SQLITE
            # =================================================

            try:

                save_transaction(
                    processed_transaction
                )

                statistics[
                    "transactions_saved"
                ] += 1

                logger.info(
                    f"Transaction saved: "
                    f"{transaction_id}"
                )

            except Exception as error:

                logger.error(
                    f"SQLite transaction "
                    f"storage error: "
                    f"{error}"
                )


            # =================================================
            # NORMAL ROUTING
            # =================================================

            if risk_level == "NORMAL":

                producer.send(

                    NORMAL_TOPIC,

                    key=card_id,

                    value=
                        processed_transaction
                )

                producer.flush()


                append_json_record(

                    NORMAL_JSON,

                    processed_transaction
                )


                print(
                    "📤 ROUTED → "
                    "normal-transactions"
                )


            # =================================================
            # SUSPICIOUS ROUTING
            # =================================================

            elif risk_level == "SUSPICIOUS":

                producer.send(

                    ANOMALY_TOPIC,

                    key=card_id,

                    value=
                        processed_transaction
                )

                producer.flush()


                append_json_record(

                    ANOMALY_JSON,

                    processed_transaction
                )


                print(
                    "📤 ROUTED → "
                    "anomaly-transactions"
                )


            # =================================================
            # HIGH RISK ROUTING
            # =================================================

            elif risk_level == "HIGH RISK":

                producer.send(

                    HIGH_RISK_TOPIC,

                    key=card_id,

                    value=
                        processed_transaction
                )

                producer.flush()


                append_json_record(

                    ANOMALY_JSON,

                    processed_transaction
                )


                # =============================================
                # CREATE HIGH-RISK ALERT
                # =============================================

                alert = (
                    create_high_risk_alert(
                        processed_transaction
                    )
                )


                # =============================================
                # SAVE ALERT JSON
                # =============================================

                append_json_record(

                    ALERT_JSON,

                    alert
                )


                # =============================================
                # SAVE ALERT DATABASE
                # =============================================

                try:

                    save_alert(
                        alert
                    )

                    statistics[
                        "alerts_generated"
                    ] += 1

                except Exception as error:

                    logger.error(
                        f"Alert database "
                        f"error: {error}"
                    )


                print(
                    "🚨 ALERT ROUTED → "
                    "high-risk-alerts"
                )


                logger.warning(

                    f"HIGH RISK ALERT | "
                    f"{transaction_id} | "
                    f"Score={risk_score} | "
                    f"Anomalies="
                    f"{', '.join(anomalies)}"
                )


            # =================================================
            # FINAL TRANSACTION RESULT
            # =================================================

            print()

            print(
                "******** TRANSACTION RESULT ********"
            )

            print(
                f"Transaction ID : "
                f"{transaction_id}"
            )

            print(
                f"Card ID        : "
                f"{card_id}"
            )

            print(
                f"Risk Score     : "
                f"{risk_score}"
            )

            print(
                f"Risk Level     : "
                f"{risk_level}"
            )


            if anomalies:

                print(
                    "Anomalies      : "
                    +
                    ", ".join(
                        anomalies
                    )
                )

            else:

                print(
                    "Anomalies      : None"
                )


            print(
                "************************************"
            )


            # =================================================
            # SAVE STATISTICS
            # =================================================

            save_statistics()


        # =====================================================
        # INDIVIDUAL TRANSACTION ERROR
        # =====================================================

        except Exception as error:

            statistics[
                "processing_errors"
            ] += 1

            logger.exception(
                f"Transaction processing "
                f"error: {error}"
            )

            print()

            print(
                f"❌ Processing error: "
                f"{error}"
            )

            save_statistics()


# ============================================================
# KEYBOARD INTERRUPT
# ============================================================

except KeyboardInterrupt:

    print()

    print(
        "Detector stopping..."
    )

    logger.info(
        "Detector stopped by user."
    )


# ============================================================
# FATAL ERROR
# ============================================================

except Exception as error:

    statistics[
        "processing_errors"
    ] += 1

    logger.exception(
        f"Fatal detector error: {error}"
    )

    print()

    print(
        f"❌ Detector error: {error}"
    )


# ============================================================
# GRACEFUL SHUTDOWN
# ============================================================

finally:

    print()

    print(
        "Saving final statistics..."
    )

    save_statistics()


    try:

        consumer.close()

    except Exception:

        pass


    try:

        producer.close()

    except Exception:

        pass


    logger.info(
        "Kafka consumer and producer "
        "closed successfully."
    )


    print()

    print(
        "========================================"
    )

    print(
        "       DETECTOR STOPPED"
    )

    print(
        "========================================"
    )

    print()