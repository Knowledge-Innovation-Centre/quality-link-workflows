if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition

import os
import psycopg2
from datetime import datetime

@condition
def need_to_generate_key(*args, **kwargs) -> bool:
    """
    Check if a key pair already exists in the backend database

    Returns:
        False if key exists (= no run needed)
        True if it does not exist (= need to generate)
    """

    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB_NAME"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        print("✅ Connected to PostgreSQL database")
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return {
            "success": False,
            "error": f"Database connection error: {e}"
        }

    print(f"\n🔍 Verifying keys in database...")

    try:
        cursor.execute("""
            SELECT
                cred_uuid,
                key_algorithm,
                key_size,
                key_format,
                created_at,
                updated_at
            FROM ql_cred
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
        """)

        result = cursor.fetchone()

        if result:
            print(f"✅ Active key found in database:")
            print(f"   UUID: {result[0]}")
            print(f"   Algorithm: {result[1]}")
            print(f"   Key Size: {result[2]}")
            print(f"   Format: {result[3]}")
            print(f"   Created: {result[4]}")
            print(f"   Updated: {result[5]}")

            # return value used as condition for next block
            return False
        else:
            print(f"❌ No active key found in database.")
            # return value used as condition for next block
            return True

    except Exception as e:
        print(f"❌ Error fetching existing keys: {e}")
        # return value used as condition for next block
        return True
