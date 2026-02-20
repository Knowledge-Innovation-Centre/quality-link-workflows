if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import psycopg2
import json
from psycopg2.extras import Json, DictCursor, execute_values
from datetime import datetime
import time
import uuid
import re

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    total_count = 0
    new_providers_count = 0
    updated_providers_count = 0
    unchanged_providers_count = 0
    error_providers_count = 0

    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB_NAME"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        print("✅ Connected to PostgreSQL database")
        cursor = conn.cursor(cursor_factory=DictCursor)
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return {
            "success": False,
            "error": f"Database connection error: {e}"
        }

    for provider in data:
        total_count += 1
        provider_id = provider.get('id')
        cursor.execute(
            "SELECT provider_uuid, metadata FROM provider WHERE base_id = %s",
            (provider_id,)
        )
        if cursor.rowcount > 0:
            existing = cursor.fetchone()
            if existing['metadata'] != provider:
                if update_provider(conn, cursor, existing['provider_uuid'], provider):
                    updated_providers_count += 1
                else:
                    error_providers_count += 1
            else:
                unchanged_providers_count += 1
        else:
            if insert_provider(conn, cursor, provider):
                new_providers_count += 1
            else:
                error_providers_count += 1

    print(f"✅ Processed batch of {len(data)} providers.")
    print(f"📊 New: {new_providers_count}\n  Updated: {updated_providers_count}\n  Unchanged: {unchanged_providers_count}\n  Errors: {error_providers_count}")

    cursor.close()
    conn.close()
    print("🔌 Database connection closed")

    run_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()

    return {
        "success": True,
        "run_id": run_id,
        "timestamp": timestamp,
        "total_count": total_count,
        "new_providers": new_providers_count,
        "unchanged_providers": unchanged_providers_count,
        "updated_providers": updated_providers_count,
        "error_providers": error_providers_count,
    }

def extract_schac_identifier(provider):
    identifiers = provider.get('identifiers', [])
    for identifier in identifiers:
        if identifier.get('resource') == 'SCHAC':
            return identifier.get('identifier')
    return None

def clean_website_url(url):
    if not url:
        return None

    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    url = url.rstrip('/')
    return url

def build_manifest_json(provider):
    manifest_json = []

    schac_id = extract_schac_identifier(provider)

    website_link = provider.get('website_link')
    clean_website = clean_website_url(website_link)

    if schac_id:
        manifest_json.append({"domain": schac_id, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": schac_id, "type": ".well-known", "check": False, "path": None})

    if website_link:
        manifest_json.append({"domain": website_link, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": website_link, "type": ".well-known", "check": False, "path": None})

    if clean_website and clean_website != website_link:
        manifest_json.append({"domain": clean_website, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": clean_website, "type": ".well-known", "check": False, "path": None})

    return manifest_json

def build_name_concat(provider):
    name_parts = []

    name_primary = provider.get('name_primary')
    if name_primary:
        name_parts.append(name_primary)

    names = provider.get('names', [])
    if names and len(names) > 0:
        name_official = names[0].get('name_official')
        if name_official:
            name_parts.append(name_official)
        name_transliterated = names[0].get('name_official_transliterated')
        if name_transliterated:
            name_parts.append(name_transliterated)
        name_english = names[0].get('name_english')
        if name_english:
            name_parts.append(name_english)
        acronym = names[0].get('acronym')
        if acronym:
            name_parts.append(acronym)

    return " ".join(name_parts)

def insert_provider(conn, cursor, provider):
    try:
        insert_data = {
            'current_time': datetime.now(),
            'provider_id': provider.get('id'),
            'deqar_id': provider.get('deqar_id'),
            'eter_id': provider.get('eter_id'),
            'schac_code': extract_schac_identifier(provider),
            'metadata': Json(provider),
            'manifest_json': Json(build_manifest_json(provider)),
            'name_concat': build_name_concat(provider),
            'provider_name': provider.get('name_primary', '')
        }
        cursor.execute(
            """
            INSERT INTO provider (
                deqar_id, eter_id, base_id, schac_code, metadata, manifest_json, 
                name_concat, provider_name, last_deqar_pull, 
                last_manifest_pull, created_at, updated_at
            ) VALUES (
                %(deqar_id)s, %(eter_id)s, %(provider_id)s, %(schac_code)s, %(metadata)s, %(manifest_json)s,
                %(name_concat)s, %(provider_name)s, %(current_time)s,
                NULL, %(current_time)s, %(current_time)s
            )
            """,
            insert_data,
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error batch inserting provider: {e}")
        conn.rollback()
        return False

def update_provider(conn, cursor, provider_uuid, provider):
    try:
        update_data = {
            'provider_uuid': provider_uuid,
            'current_time': datetime.now(),
            'provider_id': provider.get('id'),
            'deqar_id': provider.get('deqar_id'),
            'eter_id': provider.get('eter_id'),
            'schac_code': extract_schac_identifier(provider),
            'metadata': Json(provider),
            'manifest_json': Json(build_manifest_json(provider)),
            'name_concat': build_name_concat(provider),
            'provider_name': provider.get('name_primary', '')
        }
        cursor.execute("""
            UPDATE provider
            SET
                deqar_id = %(deqar_id)s,
                eter_id = %(eter_id)s,
                schac_code = %(schac_code)s,
                metadata = %(metadata)s,
                name_concat = %(name_concat)s,
                provider_name = %(provider_name)s,
                last_deqar_pull = %(current_time)s,
                updated_at = %(current_time)s
            WHERE provider.provider_uuid = %(provider_uuid)s
        """, update_data)
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error batch updating provider: {e}")
        conn.rollback()
        return False

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
    assert output.get("total_count", 0) > 0, 'No providers processed'
    assert "new_providers" in output, 'New providers count is missing'
    assert "updated_providers" in output, 'Updated providers count is missing'
    assert "unchanged_providers" in output, 'Unchanged providers count is missing'
    assert "error_providers" in output, 'Error count is missing'

    total = output.get("total_count", 0)
    new = output.get("new_providers", 0)
    updated = output.get("updated_providers", 0)
    unchanged = output.get("unchanged_providers", 0)
    errors = output.get("error_providers", 0)
    assert total == (new + updated + unchanged + errors), 'Provider counts do not add up'
