# utils/libs/mirco_services_data_management/mirco_services_data_management/db.py

import os
import psycopg2
import psycopg2.extensions
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_connection(dbname_var: str = "DB_NAME"):
    """
    Возвращает psycopg2-соединение, учитывая переменные окружения:
      DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT.
    """
    dbname = os.getenv(dbname_var, 'my_database')
    user = os.getenv("DB_USER", 'postgres')
    password = os.getenv("DB_PASSWORD", 'postgres')
    host = os.getenv("DB_HOST", 'postgres')
    port = os.getenv("DB_PORT", '5432')

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

def ensure_database_exists(target_db_var='DB_NAME', main_db='postgres'):
    """
    Проверяет/создаёт базу данных (target_db), если её нет.
    Подключается к main_db (по умолчанию postgres).
    """
    target_db = os.getenv(target_db_var, 'my_database')
    user = os.getenv("DB_USER", 'postgres')
    password = os.getenv("DB_PASSWORD", 'postgres')
    host = os.getenv("DB_HOST", 'postgres')
    port = os.getenv("DB_PORT", '5432')

    conn = psycopg2.connect(
        dbname=main_db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
            exists = cur.fetchone()
            if not exists:
                logger.info(f"Database '{target_db}' not found. Creating...")
                cur.execute(f"CREATE DATABASE {target_db};")
                logger.info(f"Database '{target_db}' created successfully!")
            else:
                logger.info(f"Database '{target_db}' already exists.")
    finally:
        conn.close()

def ensure_processed_table(table_name="processed_messages", unique_field="message_id"):
    """
    Создаёт таблицу, где храним записи об обработанных сообщениях (чтобы избежать дублей).
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {unique_field} BIGINT PRIMARY KEY,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
        logger.info(f"Таблица '{table_name}' проверена/создана.")
    finally:
        conn.close()

def is_processed(message_id: int, table_name="processed_messages", unique_field="message_id") -> bool:
    """
    Проверяет, есть ли запись с message_id в таблице processed_messages.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {table_name} WHERE {unique_field} = %s", (message_id,))
            row = cur.fetchone()
            return row is not None
    except Exception as e:
        logger.warning(f"Error checking {table_name} for message_id={message_id}: {e}")
        return False
    finally:
        conn.close()

def mark_processed(message_id: int, table_name="processed_messages", unique_field="message_id"):
    """
    Помечает сообщение как обработанное, вставляя запись (с ON CONFLICT DO NOTHING).
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {table_name} ({unique_field})
                    VALUES (%s)
                    ON CONFLICT DO NOTHING;
                """, (message_id,))
    except Exception as e:
        logger.error(f"Error marking message_id={message_id} as processed: {e}")
    finally:
        conn.close()

# ------------------- Пример партиционирования -------------------

def ensure_partitioned_parent_table(parent_table, unique_index_fields=None):
    """
    Создаёт партиционированную таблицу (PARTITION BY RANGE (month_part)).
    Например:
        CREATE TABLE parent_table (
            id SERIAL,
            data JSONB NOT NULL,
            month_part DATE NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, month_part)
        ) PARTITION BY RANGE (month_part);
    И при необходимости создаём уникальный индекс (unique_index_fields).
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {parent_table} (
                        id SERIAL,
                        data JSONB NOT NULL,
                        month_part DATE NOT NULL,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (id, month_part)
                    )
                    PARTITION BY RANGE (month_part);
                """)
                logger.info(f"Таблица {parent_table} PARTITION BY RANGE проверена/создана.")

                if unique_index_fields:
                    idx_name = f"{parent_table}_{'_'.join(unique_index_fields)}_uniq_idx"
                    fields_str = ", ".join(unique_index_fields)
                    cur.execute(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                        ON {parent_table} ({fields_str});
                    """)
                    logger.info(f"Уникальный индекс {idx_name} проверен/создан.")
    finally:
        conn.close()

def ensure_partition_exists(parent_table: str, month_part: datetime):
    """
    Создаёт (если нет) партицию вида parent_table_YYYY_MM с границами [start_of_month, start_of_next).
    """
    conn = get_connection()
    partition_name = f"{parent_table}_{month_part.strftime('%Y_%m')}"
    start_date = datetime(year=month_part.year, month=month_part.month, day=1)
    if month_part.month == 12:
        end_date = datetime(year=month_part.year + 1, month=1, day=1)
    else:
        end_date = datetime(year=month_part.year, month=month_part.month + 1, day=1)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s);", (partition_name,))
                exists = cur.fetchone()[0]
                if not exists:
                    logger.info(f"Создаём партицию {partition_name}...")
                    cur.execute(f"""
                        CREATE TABLE {partition_name}
                        PARTITION OF {parent_table}
                        FOR VALUES FROM (%s) TO (%s);
                    """, (start_date.date(), end_date.date()))
                    logger.info(f"Партиция {partition_name} успешно создана.")
    finally:
        conn.close()

def insert_partitioned_record(parent_table: str, data_dict: dict, deduplicate=True) -> bool:
    """
    Вставляет data_dict в JSONB‑поле, month_part = 1 число текущего месяца.
    Если deduplicate=True, используется ON CONFLICT DO NOTHING (но требуется уникальный индекс).
    Возвращает True, если запись вставлена (не дубликат), False если дубликат.
    """
    month_part = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    ensure_partition_exists(parent_table, month_part)
    conn = get_connection()
    inserted = False
    try:
        with conn:
            with conn.cursor() as cur:
                if deduplicate:
                    sql = f"""
                        INSERT INTO {parent_table} (data, month_part)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id;
                    """
                else:
                    sql = f"""
                        INSERT INTO {parent_table} (data, month_part)
                        VALUES (%s, %s)
                        RETURNING id;
                    """

                cur.execute(sql, (json.dumps(data_dict), month_part.date()))
                row = cur.fetchone()
                if row is not None:
                    inserted = True
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при вставке в {parent_table}: {e}")
    finally:
        conn.close()
    return inserted
