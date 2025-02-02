
# utils/libs/mirco_services_data_management/setup.py

from setuptools import setup, find_packages

setup(
    name="mirco_services_data_management",
    version="0.1.0",
    author="Andrei Grishin",
    description="Внутренняя библиотека для микросервисов, включает в себя "
                "(Kafka, PostgreSQL, дубли, партиционирование).",
    packages=find_packages(),  # автоматически найдёт mirco_services_data_management/
    install_requires=[
        "kafka-python",
        "psycopg2-binary"
    ],
    python_requires=">=3.8"
)
