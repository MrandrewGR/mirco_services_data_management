#   setup.py

from setuptools import setup, find_packages

setup(
    name="mirco_services_data_management",  # Keep the importable name
    version="0.2.3",  # Bumped for the new index fixes
    author="Andrei Grishin",
    author_email="",
    description=(
        "Внутренняя библиотека для микросервисов (Kafka aio, PostgreSQL, партиционирование, дубли)."
    ),
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        "aiokafka>=0.8.6",
        "psycopg2-binary>=2.9.5"
    ],
    python_requires=">=3.8",
    include_package_data=True,
)
