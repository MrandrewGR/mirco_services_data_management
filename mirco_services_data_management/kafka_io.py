# mirco_services_data_management/kafka_io.py

import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)

async def create_consumer(topic: str,
                          bootstrap_servers: str,
                          group_id: str):
    """
    Создаёт асинхронный Kafka Consumer.
    Не забудьте вызвать await consumer.start() перед использованием.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True  # or False if you want manual commits
    )
    return consumer

async def create_producer(bootstrap_servers: str,
                          transactional: bool = False,
                          transactional_id: str = None):
    """
    Создаёт асинхронный Kafka Producer.
    Если transactional=True, нужно указать transactional_id.
    Не забудьте вызвать await producer.start() перед использованием.
    """
    if transactional and not transactional_id:
        raise ValueError("Transactional is True but no transactional_id provided.")

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        transactional_id=transactional_id if transactional else None,
        # If you want idempotent producer but not necessarily transactions:
        # acks='all', enable_idempotence=True
    )
    return producer

async def send_message(producer: AIOKafkaProducer,
                       topic: str,
                       message: dict):
    """
    Отправляет сообщение (dict) в указанную тему Kafka.
    Если используется транзакционный продюсер, вызывать в транзакции,
    перед commit/abort.
    """
    try:
        # aiokafka needs bytes, so we can encode the dict as JSON
        import json
        data = json.dumps(message).encode('utf-8')
        fut = await producer.send_and_wait(topic, data)
        logger.debug(f"Message sent to Kafka topic={topic}, offset={fut.offset}")
    except Exception as e:
        logger.error(f"Error sending message to Kafka: {e}")
        raise
