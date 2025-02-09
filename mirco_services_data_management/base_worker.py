# mirco_services_data_management/base_worker.py

import asyncio
import time
import logging

from .config import BaseConfig
from .logger import setup_logging
from .db import is_processed, mark_processed
from .kafka_io import create_consumer, create_producer, send_message

logger = logging.getLogger(__name__)

class BaseWorker:
    """
    Асинхронный базовый класс для микросервисов.
    Использует aiokafka для работы с Kafka.
    """
    def __init__(self, config: BaseConfig):
        self.config = config
        setup_logging(self.config.LOG_LEVEL)
        logger.info("BaseWorker: инициализация.")

        # Заполняем поля, но создадим consumer/producer в async контексте
        self.consumer = None
        self.producer = None
        self._stop_event = asyncio.Event()

    async def start(self):
        """
        Запуск асинхронного цикла воркера:
         1) Создаём consumer и producer.
         2) Запускаем задачи (Kafka loop, DB poll loop).
         3) Ожидаем сигнала завершения.
        """
        logger.info("BaseWorker.start() called.")
        # Инициализация consumer/producer
        self.consumer = await create_consumer(
            topic=self.config.KAFKA_CONSUME_TOPIC,
            bootstrap_servers=self.config.KAFKA_BROKER,
            group_id=self.config.KAFKA_GROUP_ID
        )
        self.producer = await create_producer(
            bootstrap_servers=self.config.KAFKA_BROKER,
            transactional=self.config.TRANSACTIONAL,
            transactional_id=self.config.TRANSACTIONAL_ID
        )

        # Запуск consumer / producer
        await self.consumer.start()
        await self.producer.start()

        # Если транзакционный продюсер — инициализируем транзакции
        if self.config.TRANSACTIONAL:
            logger.info(f"Initializing transactional producer with id={self.config.TRANSACTIONAL_ID}")
            # Note: aiokafka automatically calls init_transactions() on producer.start()
            # If needed, you can ensure or confirm here.

        # Запускаем задачи
        kafka_task = asyncio.create_task(self.run_kafka_loop(), name="kafka_task")
        db_poll_task = asyncio.create_task(self.poll_db_loop(), name="db_poll_task")

        # Ожидаем сигнал остановки
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Завершаем все задачи...")
            kafka_task.cancel()
            db_poll_task.cancel()
            await asyncio.gather(kafka_task, db_poll_task, return_exceptions=True)
            await self.shutdown()

    async def shutdown(self):
        """
        Корректно останавливаем consumer, producer
        """
        logger.info("BaseWorker.shutdown() called.")
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    def stop(self):
        """
        Вызываем из внешнего кода, чтобы инициировать завершение.
        """
        self._stop_event.set()

    async def run_kafka_loop(self):
        """
        Асинхронная обработка входящих сообщений из Kafka.
        """
        logger.info("Начат run_kafka_loop")
        try:
            async for msg in self.consumer:
                data = self.decode_message(msg.value)
                await self.handle_message(data)
        except asyncio.CancelledError:
            logger.info("run_kafka_loop cancelled.")
        except Exception as e:
            logger.error(f"Ошибка в run_kafka_loop: {e}", exc_info=True)

    def decode_message(self, raw_value: bytes) -> dict:
        """
        Декодируем байты из Kafka в dict (JSON).
        """
        import json
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            logger.warning("Не удалось декодировать сообщение как JSON.")
            return {}

    async def poll_db_loop(self):
        """
        Асинхронный опрос БД через заданные интервалы self.config.POLL_INTERVAL_DB.
        """
        logger.info("Начат poll_db_loop")
        try:
            while True:
                await self.check_db_for_unprocessed()
                await asyncio.sleep(self.config.POLL_INTERVAL_DB)
        except asyncio.CancelledError:
            logger.info("poll_db_loop cancelled.")

    async def check_db_for_unprocessed(self):
        """
        Заглушка: унаследованный класс может переопределить эту функцию
        для выполнения backfill / обработки непрочитанных записей в БД.
        """
        logger.debug("check_db_for_unprocessed() - нужно переопределить (async).")

    async def handle_message(self, message: dict):
        """
        Обработка входящего Kafka-сообщения.
        При транзакционном подходе: начинаем транзакцию, обрабатываем, коммит или аборт.
        """
        msg_id = self.extract_message_id(message)
        if msg_id is None:
            logger.warning("Не найден ID в сообщении, пропускаем.")
            return

        if is_processed(msg_id):
            logger.info(f"Сообщение {msg_id} уже обработано, пропускаем.")
            return

        # Если транзакционный подход, начинаем транзакцию
        if self.config.TRANSACTIONAL:
            await self.producer.begin_transaction()
            logger.debug("Kafka transaction started.")

        try:
            await self.process_message(message)
            # Помечаем сообщение как обработанное (синхронный DB вызов).
            mark_processed(msg_id)

            # Завершаем транзакцию
            if self.config.TRANSACTIONAL:
                await self.producer.commit_transaction()
                logger.debug("Kafka transaction committed.")

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения {msg_id}: {e}", exc_info=True)
            if self.config.TRANSACTIONAL:
                await self.producer.abort_transaction()
                logger.debug("Kafka transaction aborted.")

    def extract_message_id(self, message: dict):
        return message.get("message_id") or message.get("id")

    async def process_message(self, message: dict):
        """
        Метод, который реально обрабатывает сообщение.
        Если нужно отправить результат в другую тему, используйте send_message().
        Можно переопределить этот метод.
        """
        logger.info(f"BaseWorker: обрабатываем сообщение: {message}")

        # Пример отправки сообщения (non-transactional or within transaction)
        # await send_message(self.producer, self.config.KAFKA_PRODUCE_TOPIC, {"processed": message})
        # logger.debug("Sent processed message to output topic.")
