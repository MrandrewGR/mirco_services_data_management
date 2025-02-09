#   mirco_services_data_management/config.py

import os

class BaseConfig:
    """
    Базовая конфигурация, считываемая из переменных окружения.
    """
    # Kafka
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
    KAFKA_CONSUME_TOPIC = os.getenv('KAFKA_CONSUME_TOPIC', 'my_input_topic')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'my_worker_group')
    KAFKA_PRODUCE_TOPIC = os.getenv('KAFKA_PRODUCE_TOPIC', 'my_output_topic')

    # Optional: Kafka transactions
    TRANSACTIONAL = os.getenv('TRANSACTIONAL', 'False').lower() in ('true', '1', 'yes')
    TRANSACTIONAL_ID = os.getenv('TRANSACTIONAL_ID', 'my_transaction_id')

    # Postgres
    DB_HOST = os.getenv('DB_HOST', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_NAME = os.getenv('DB_NAME', 'my_database')

    # Интервал опроса БД (backfill), в секундах
    POLL_INTERVAL_DB = int(os.getenv('POLL_INTERVAL_DB', '60'))

    # Уровень логирования (DEBUG, INFO, WARNING, etc.)
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
