from prefect import task

import os
import psycopg2
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import hashlib
from datetime import datetime


@task(name="save_key_pair_db")
def save_key_pair_db(key_size: int = 4096, public_exponent: int = 65537):

    print("🔑 Generating private key (4096-bit RSA)...")
    private_key = rsa.generate_private_key(
        public_exponent=public_exponent,
        key_size=key_size,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    private_pem_str = private_pem.decode("utf-8")
    public_pem_str = public_pem.decode("utf-8")
    fingerprint = hashlib.sha256(public_pem).hexdigest()

    print(f"✅ Keys serialized successfully")
    print(f"   Private key length: {len(private_pem_str)} characters")
    print(f"   Public key length: {len(public_pem_str)} characters")

    print(f"💾 Inserting keys into ql_cred table...")

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

    try:
        # mark any existing keys as inactive
        cursor.execute("UPDATE ql_cred SET is_active = false;")
        # insert newly generated key
        cursor.execute("""
            INSERT INTO ql_cred
            (public_key, private_key, key_algorithm, key_size, public_exponent, key_format)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING cred_uuid, created_at;
        """, (
            public_pem_str,
            private_pem_str,
            'RSA',
            key_size,
            public_exponent,
            'PEM'
        ))
        cred_uuid, created_at = cursor.fetchone()
        conn.commit()

        print(f"✅ Key saved successfully:")
        print(f"   Credential UUID: {cred_uuid}")
        print(f"   Created at: {created_at}")
        print(f"   Fingerprint: {fingerprint}")

    except Exception as e:
        print(f"❌ Error inserting keys: {e}")
        conn.rollback()

    print(f"\n🔌 Closing database connection...")
    cursor.close()
    conn.close()
