import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, Summary, start_http_server

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
SCORE_BUCKETS = [i / 50.0 for i in range(51)]

WRITE_ERRORS = Counter('scoring_write_errors_total', 'Ошибки записи результатов')
DB_WRITE_TIME = Summary('scoring_db_write_seconds', 'Время записи скоринга в БД')
SCORING_COUNT = Counter('scorings_total', 'Общее количество записанных скорингов')
FRAUD_SCORE_HISTOGRAM = Histogram(
    'scoring_fraud_score',
    'Распределение записанных скоров мошенничества',
    buckets=SCORE_BUCKETS,
)
FRAUD_COUNT = Counter(
    'fraud_detected_total',
    'Количество обнаруженных мошеннических транзакций',
)


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id SERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                score FLOAT NOT NULL,
                fraud_flag INT NOT NULL,
                model_version TEXT,
                us_state TEXT,
                merch TEXT,
                cat_id TEXT,
                amount DOUBLE PRECISION,
                scored_at TIMESTAMPTZ,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            -- Дополняем и существующую таблицу, чтобы не удалять старый том.
            ALTER TABLE scores
                ADD COLUMN IF NOT EXISTS model_version TEXT,
                ADD COLUMN IF NOT EXISTS us_state TEXT,
                ADD COLUMN IF NOT EXISTS merch TEXT,
                ADD COLUMN IF NOT EXISTS cat_id TEXT,
                ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;
            CREATE INDEX IF NOT EXISTS scores_time_idx ON scores (scored_at);
            CREATE INDEX IF NOT EXISTS scores_state_merch_time_idx
                ON scores (us_state, merch, scored_at);
        """)
        conn.commit()
        logger.info("Таблица 'scores' проверена или создана.")


@DB_WRITE_TIME.time()
def insert_score(conn, data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scores (
                transaction_id, score, fraud_flag, model_version,
                us_state, merch, cat_id, amount, scored_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            data["transaction_id"],
            data["score"],
            data["fraud_flag"],
            data.get("model_version"),
            data.get("us_state"),
            data.get("merch"),
            data.get("cat_id"),
            data.get("amount"),
            # Старые сообщения тоже можно записать, но бизнес-признаков в них нет.
            data.get("scored_at") or datetime.now(timezone.utc),
        ))
        conn.commit()

    SCORING_COUNT.inc()
    FRAUD_SCORE_HISTOGRAM.observe(data["score"])
    if data["fraud_flag"] == 1:
        FRAUD_COUNT.inc()


def run_consumer():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    scoring_topic = os.getenv("KAFKA_SCORING_TOPIC")

    consumer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'scoring-writer',
        'auto.offset.reset': 'earliest',
    }

    logger.info(f"Подключение к Kafka: {kafka_bootstrap_servers}, топик: {scoring_topic}")
    consumer = Consumer(consumer_config)
    consumer.subscribe([scoring_topic])

    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    create_table(conn)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.debug("Ожидание сообщений...")
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                value_str = msg.value().decode('utf-8')
                logger.debug(f"Получено сообщение: {value_str}")
                data_list = json.loads(value_str)

                # Проверяем, что это список
                if isinstance(data_list, list):
                    for data in data_list:
                        required_keys = ['transaction_id', 'score', 'fraud_flag']
                        if not all(k in data for k in required_keys):
                            logger.error(f"Некорректный формат элемента: {data}")
                            continue
                        insert_score(conn, data)
                        logger.info(f"Записано в БД: {data['transaction_id']}")
                else:
                    logger.error(f"Ожидался список, получен: {type(data_list)}")

            except json.JSONDecodeError as je:
                logger.exception(f"Ошибка декодирования JSON: {je}")
            except Exception as e:
                # После SQL-ошибки снимаем состояние aborted у соединения
                conn.rollback()
                WRITE_ERRORS.inc()
                logger.exception(f"Ошибка обработки сообщения: {e}")

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    logger.info("Запуск потребителя и писателя в БД...")
    run_consumer()
