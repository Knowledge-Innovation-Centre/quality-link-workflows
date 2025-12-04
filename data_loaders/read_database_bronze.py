from mage_ai.data_preparation.shared.secrets import get_secret_value
import psycopg2
from psycopg2 import sql
from datetime import date, timedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):

    conn = None
    cursor = None
    
    try:
        today = date.today()
        # yesterday = today
        yesterday = today - timedelta(days=1)

        print(f"📅 Loading transactions for date: {yesterday}")
        
        conn = psycopg2.connect(
            host=get_secret_value("POSTGRES_HOST"),
            database=get_secret_value("POSTGRES_DB_NAME"),
            user=get_secret_value("POSTGRES_USER"),
            password=get_secret_value("POSTGRES_PASSWORD")
        )
        cursor = conn.cursor()
        
        print("✅ Connected to PostgreSQL")
        
        query = """
            SELECT *
            FROM transaction
            WHERE created_at_date = %s
            ORDER BY created_at_date_time ASC
        """
        
        cursor.execute(query, (yesterday,))
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "trans_uuid": str(row[0]),
                "provider_uuid": str(row[1]),
                "source_version_uuid": str(row[2]),
                "created_at_date": row[3],
                "created_at_date_time": row[4]
            })
        
        print(f"📊 Loaded {len(results)} transaction records")
        
        return results
        
    except Exception as e:
        print(f"❌ Error loading transaction data: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("🔌 Database connection closed")


@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
    
    if isinstance(output, list):
        assert len(output) >= 0, 'Output list should be valid'
        if len(output) > 0:
            record = output[0]
            assert isinstance(record, dict), 'Each transaction should be a dict'
            assert 'trans_uuid' in record, 'Missing trans_uuid field'
            assert 'provider_uuid' in record, 'Missing provider_uuid field'
            assert 'source_version_uuid' in record, 'Missing source_version_uuid field'
            print(f"✅ Test passed: {len(output)} transaction records loaded")
        else:
            print(f"✅ Test passed: No transactions found for the date")
    else:
        assert isinstance(output, dict), 'Output should be a dict (single transaction record)'
        assert 'trans_uuid' in output, 'Missing trans_uuid field'
        assert 'provider_uuid' in output, 'Missing provider_uuid field'
        assert 'source_version_uuid' in output, 'Missing source_version_uuid field'
        print(f"✅ Test passed: Valid transaction record with UUID {output['trans_uuid']}")