# mirco_services_data_management/base_worker.py

import threading
import time
import logging
from .config import BaseConfig
from .logger import setup_logging
from .db import is_processed, mark_processed
from .kafka_io import create_consumer, create_producer, send_message

logger = logging.getLogger(__name__)

class BaseWorker:
    """
    Универсальный базовый класс, который можно переопределять
    в конкретном микросервисе
    """
    def __init__(self, config: BaseConfig):
        self.config = config
        # Настраиваем логирование
        setup_logging(self.config.LOG_LEVEL)
        logger.info("BaseWorker: инициализация.")

        # Создаём Kafka consumer / producer
        self.consumer = create_consumer(
            topic=self.config.KAFKA_CONSUME_TOPIC,
            bootstrap_servers=self.config.KAFKA_BROKER,
            group_id=self.config.KAFKA_GROUP_ID
        )
        self.producer = create_producer(self.config.KAFKA_BROKER)

    def start(self):
        logger.info("BaseWorker.start() called. Запускаем потоки.")
        t_kafka = threading.Thread(target=self.run_kafka_loop, daemon=True)
        t_kafka.start()

        t_db = threading.Thread(target=self.poll_db_loop, daemon=True)
        t_db.start()

        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания, завершаем.")

    def run_kafka_loop(self):
        for msg in self.consumer:
            data = msg.value
            self.handle_message(data)

    def poll_db_loop(self):
        while True:
            self.check_db_for_unprocessed()
            time.sleep(self.config.POLL_INTERVAL_DB)

    def check_db_for_unprocessed(self):
        logger.debug("check_db_for_unprocessed() - нужно переопределить.")

    def handle_message(self, message: dict):
        msg_id = self.extract_message_id(message)
        if msg_id is None:
            logger.warning("Не найден ID в сообщении, пропускаем.")
            return

        if is_processed(msg_id):
            logger.info(f"Сообщение {msg_id} уже обработано, пропускаем.")
            return

        self.process_message(message)
        mark_processed(msg_id)

    def extract_message_id(self, message: dict):
        return message.get("message_id") or message.get("id")

    def process_message(self, message: dict):
        logger.info(f"BaseWorker: обрабатываем сообщение: {message}")
