# File: mirco_services_data_management/db.py

import os
import psycopg2
import psycopg2.extensions
import json
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)

def get_connection(dbname_var: str = "DB_NAME"):
    """
    Returns a psycopg2 connection using environment variables:
      - DB_NAME (or the provided dbname_var)
      - DB_USER, DB_PASSWORD, DB_HOST, DB_PORT.
    """
    dbname = os.getenv(dbname_var)  # Dynamically fetch from environment variable
    user = os.getenv("DB_USER", 'postgres')
    password = os.getenv("DB_PASSWORD", 'postgres')
    host = os.getenv("DB_HOST", 'postgres')
    port = os.getenv("DB_PORT", '5432')

    if not dbname:
        raise ValueError(f"Database name '{dbname_var}' is not set in the environment variables.")

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )


def ensure_database_exists(target_db_var='DB_NAME', main_db='postgres'):
    """
    Checks if the target database exists and creates it if not.
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
    Creates the table to store processed messages to avoid duplicates.
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
        logger.info(f"Table '{table_name}' checked/created.")
    finally:
        conn.close()


def is_processed(message_id: int, table_name="processed_messages", unique_field="message_id") -> bool:
    """
    Checks if a given message_id already exists in the processed_messages table.
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
    Marks a message as processed by inserting a record into the processed_messages table.
    Uses ON CONFLICT DO NOTHING to avoid duplicates.
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


def ensure_partitioned_parent_table(parent_table: str):
    """
    Creates a partitioned table (PARTITION BY RANGE on month_part) if not exists,
    with a dedicated column for message_id that we can use for upsert.
    The primary key is (month_part, message_id).
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Create the parent partitioned table if needed
                # We store message_id as a BIGINT, plus a unique constraint for upsert
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {parent_table} (
                    message_id BIGINT NOT NULL,
                    data JSONB NOT NULL,
                    month_part DATE NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (month_part, message_id)
                )
                PARTITION BY RANGE (month_part);
                """)
                logger.info(f"Table {parent_table} (partitioned by month_part) checked/created.")
    finally:
        conn.close()


def ensure_partition_exists(parent_table: str, month_part: datetime):
    """
    Creates a partition (if it does not exist) named parent_table_YYYY_MM with boundaries
    [start_of_month, start_of_next_month).
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
                    logger.info(f"Creating partition {partition_name}...")
                    cur.execute(f"""
                    CREATE TABLE {partition_name}
                    PARTITION OF {parent_table}
                    FOR VALUES FROM (%s) TO (%s);
                    """, (start_date.date(), end_date.date()))
                    logger.info(f"Partition {partition_name} created successfully.")
    finally:
        conn.close()

def upsert_partitioned_record(parent_table: str, data_dict: dict):
    """
    Upserts (month_part, message_id) => data.
    Overwrites old state so that we only keep the latest version of each message_id.
    Returns True if newly inserted, False if updated existing row.
    """
    from datetime import datetime

    # Ensure we have a date-based partition corresponding to the message date (or current month).
    # Usually, you'll use the message's date (already in Moscow time) to figure out the correct month_part.
    # If you store it in data_dict["date"], parse that back out. For demonstration, let's parse:
    # e.g. "date": "2025-03-09T12:05:00+03:00"
    # Or if you want the actual posted month, do so. Otherwise fallback to "UTC now" or "Moscow now".
    import dateutil.parser

    iso_dt = data_dict.get("date")  # the date in ISO format, e.g. "2025-03-09T..."
    if iso_dt:
        dt_moscow = dateutil.parser.isoparse(iso_dt)
    else:
        # fallback if missing
        dt_moscow = datetime.now()

    # we only need year+month from dt_moscow
    month_part = dt_moscow.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    message_id = data_dict.get("message_id")
    if not message_id:
        logger.warning("upsert_partitioned_record called without message_id in data_dict!")
        return False

    # Ensure partition is created
    ensure_partition_exists(parent_table, month_part)

    inserted = False
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                import json
                # We do ON CONFLICT on the primary key (month_part, message_id),
                # and overwrite data with the new "latest" state.
                sql = f"""
                INSERT INTO {parent_table} (month_part, message_id, data)
                VALUES (%s, %s, %s)
                ON CONFLICT (month_part, message_id)
                DO UPDATE SET
                    data = EXCLUDED.data,
                    processed_at = CURRENT_TIMESTAMP
                RETURNING xmax = 0;  -- 'xmax=0' is true if newly inserted, false if updated
                """
                cur.execute(
                    sql,
                    (month_part.date(), message_id, json.dumps(data_dict))
                )
                # In Postgres, "RETURNING xmax=0" can help detect insert vs update:
                row = cur.fetchone()
                # row[0] == True => inserted, False => updated
                if row and row[0] is True:
                    inserted = True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error upserting into {parent_table}: {e}")
    finally:
        conn.close()

    return inserted


def insert_partitioned_record(parent_table: str, data_dict: dict, deduplicate=True) -> bool:
    """
    Inserts data_dict into the JSONB column of the specified partitioned table.
    The month_part is set to the first day of the current UTC month.

    If deduplicate=True, uses ON CONFLICT DO NOTHING. This REQUIRES a matching unique index,
    but we've removed it. So with deduplicate=True, you might simply skip it or rely
    on some partial index if you created one manually.

    Returns True if the record was inserted, otherwise False.
    """
    from datetime import datetime
    month_part = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    ensure_partition_exists(parent_table, month_part)
    conn = get_connection()
    inserted = False
    try:
        with conn:
            with conn.cursor() as cur:
                import json
                if deduplicate:
                    # If no unique index is defined in DB, this ON CONFLICT won't do anything.
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
        logger.error(f"Error inserting into {parent_table}: {e}")
    finally:
        conn.close()
    return inserted
