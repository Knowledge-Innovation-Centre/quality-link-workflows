# !pip3 install cryptography
import os
import psycopg2
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import hashlib
from datetime import datetime

# =============================================================================
# Configuration
# =============================================================================

pg_user = os.environ.get("POSTGRES_USER")
pg_pass = os.environ.get("POSTGRES_PASSWORD")
pg_host = os.environ.get("POSTGRES_HOST")
pg_db = os.environ.get("POSTGRES_DB_NAME")

DB_CONFIG = {
    "host": pg_host,
    "port": 5432,
    "database": "backend",
    "user": pg_user,
    "password": pg_pass
}

print("=" * 60)
print("QL-Pipeline Key Generation and Storage")
print("=" * 60)

# =============================================================================
# Step 1: Generate RSA Key Pair
# =============================================================================

print("\n🔑 Generating QL-Pipeline's private key (4096-bit RSA)...")

private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=4096,
    backend=default_backend()
)

public_key = private_key.public_key()

print(f"✅ Private key generated successfully")

# =============================================================================
# Step 2: Serialize Keys to PEM Format
# =============================================================================

print("\n📝 Serializing keys to PEM format...")

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

print(f"✅ Keys serialized successfully")
print(f"   Private key length: {len(private_pem_str)} characters")
print(f"   Public key length: {len(public_pem_str)} characters")

# =============================================================================
# Step 3: Generate Fingerprint (for logging only)
# =============================================================================

print("\n🔍 Generating fingerprint...")

fingerprint = hashlib.sha256(public_pem).hexdigest()

print(f"✅ Public key fingerprint: {fingerprint[:16]}...")

# =============================================================================
# Step 4: Connect to Database
# =============================================================================

print(f"\n🔌 Connecting to database 'backend'...")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"✅ Connected successfully")
    
except Exception as e:
    print(f"❌ Error connecting to database: {e}")
    exit(1)

# =============================================================================
# Step 5: Insert Keys into Database
# =============================================================================

print(f"\n💾 Inserting keys into ql_cred table...")

try:
    # Insert query matching your actual table schema
    insert_query = """
        INSERT INTO ql_cred 
        (public_key, private_key, key_algorithm, key_size, public_exponent, key_format)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING cred_uuid, created_at;
    """
    
    cursor.execute(insert_query, (
        public_pem_str,
        private_pem_str,
        'RSA',
        4096,
        65537,
        'PEM'
    ))
    
    cred_uuid, created_at = cursor.fetchone()
    
    conn.commit()
    
    print(f"✅ Keys inserted successfully!")
    print(f"   📋 Credential UUID: {cred_uuid}")
    print(f"   🕐 Created at: {created_at}")
    print(f"   🔍 Fingerprint: {fingerprint[:16]}...")
    
except Exception as e:
    print(f"❌ Error inserting keys: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# =============================================================================
# Step 6: Verify Keys in Database
# =============================================================================

print(f"\n🔍 Verifying keys in database...")

try:
    verify_query = """
        SELECT 
            cred_uuid,
            key_algorithm,
            key_size,
            public_exponent,
            key_format,
            is_active,
            created_at,
            LENGTH(public_key) as public_key_length,
            LENGTH(private_key) as private_key_length
        FROM ql_cred 
        WHERE cred_uuid = %s
    """
    
    cursor.execute(verify_query, (cred_uuid,))
    result = cursor.fetchone()
    
    if result:
        print(f"✅ Key found in database:")
        print(f"   UUID: {result[0]}")
        print(f"   Algorithm: {result[1]}")
        print(f"   Key Size: {result[2]}")
        print(f"   Public Exponent: {result[3]}")
        print(f"   Format: {result[4]}")
        print(f"   Active: {result[5]}")
        print(f"   Created: {result[6]}")
        print(f"   Public key length: {result[7]} chars")
        print(f"   Private key length: {result[8]} chars")
    else:
        print(f"❌ Key not found!")
    
except Exception as e:
    print(f"❌ Error verifying keys: {e}")

# =============================================================================
# Step 7: Close Database Connection
# =============================================================================

print(f"\n🔌 Closing database connection...")

cursor.close()
conn.close()

print(f"✅ Connection closed")

# =============================================================================
# Complete
# =============================================================================

print("\n" + "=" * 60)
print("✅ Process completed successfully!")
print("=" * 60)
print(f"\nYour credential UUID: {cred_uuid}")
print(f"Fingerprint (for reference): {fingerprint}")
print("Store this UUID safely for future reference!")
print("=" * 60)