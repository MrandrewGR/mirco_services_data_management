
#   mirco_services_data_management/setup.py

from setuptools import setup, find_packages

setup(
    name="mirco_services_data_management",  # Имя для импорта — с подчёркиваниями!
    version="0.1.0",
    author="Andrei Grishin",
    author_email="",
    description=(
        "Внутренняя библиотека для микросервисов, включает в себя "
        "(Kafka, PostgreSQL, дубли, партиционирование)."
    ),
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        "kafka-python>=2.0.2",
        "psycopg2-binary>=2.9.5"
    ],
    python_requires=">=3.8",
    include_package_data=True,
)
