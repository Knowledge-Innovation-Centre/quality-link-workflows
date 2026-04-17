from prefect import task

import os
from typing import Dict, List
import psycopg2
from psycopg2 import errors


@task(name="write_transaction_db")
def write_transaction_db(messages: List[Dict]):

    if not messages:
        print("⚠️ No messages to write")
        return

    db_config = {
        "host": os.environ.get("POSTGRES_HOST"),
        "database": os.environ.get("POSTGRES_DB_NAME"),
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD")
    }

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        cursor = conn.cursor()

        print(f"✅ Connected to PostgreSQL for batch write ({len(messages)} messages)")

        for msg in messages:
            if msg is None:
                print("⚠️ Skipping None message")
                continue

            try:
                provider_uuid = msg.get("provider_uuid")
                source_version_uuid = msg.get("source_version_uuid")
                source_type = msg.get("source_type", "unknown")

                if not provider_uuid or not source_version_uuid:
                    print(f"⚠️ Skipping message with missing fields: {msg}")
                    continue

                cursor.execute("BEGIN")

                insert_query = """
                    INSERT INTO transaction (provider_uuid, source_version_uuid)
                    VALUES (%s, %s)
                    RETURNING trans_uuid
                """

                cursor.execute(insert_query, (provider_uuid, source_version_uuid))
                trans_uuid = cursor.fetchone()[0]

                conn.commit()
                print(f"💾 Created transaction record: {trans_uuid} (Provider: {provider_uuid}, Source type: {source_type})")

            except errors.UniqueViolation:
                conn.rollback()
                print(f"ℹ️ Transaction already exists for provider {provider_uuid}, version {source_version_uuid} today - skipping")
                continue

            except Exception as e:
                conn.rollback()
                print(f"❌ Error inserting transaction record: {e}")
                continue

    except Exception as e:
        print(f"❌ Database connection error: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("🔌 Database connection closed")
