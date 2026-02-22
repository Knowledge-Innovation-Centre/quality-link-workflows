import os
from typing import Dict, List
from minio import Minio
from minio.error import S3Error
import json
from datetime import datetime
import requests
from io import BytesIO
from urllib.parse import urlparse
import psycopg2
from contextlib import closing

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

# need to add new data source types here
from ql.source_types.base import DataSourceType
from ql.source_types.elm import ElmDataSource
from ql.source_types.ooapi import OoapiDataSource

HANDLERS = {
    "elm": ElmDataSource,
    "ooapi": OoapiDataSource,
    #"edu-api": EduApiDataSource,
    #"occapi": OccapiDataSource,
}


@transformer
def transform(message: Dict, *args, **kwargs):

    today = datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    datetime_str = today.strftime("%Y%m%d_%H%M%S")

    try:
        client = Minio(
            os.environ.get("MINIO_HOST"),
            access_key=os.environ.get("MINIO_ROOT_USER"),
            secret_key=os.environ.get("MINIO_ROOT_PASSWORD"),
            secure=False
        )
        bucket_name = os.environ.get("MINIO_BUCKET_NAME")
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"📁 Created bucket: {bucket_name}")
        else:
            print(f"✅ Connected to MinIO, bucket: {bucket_name}")

    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")
        return None

    try:
        pg_conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB_NAME"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"⚠️ PostgreSQL connection failed: {e}")
        return None

    with closing(pg_conn):
        source = {
            'uuid': message["source_uuid"],
            'source_version_uuid': message["source_version_uuid"],
            'provider_uuid': message["provider_uuid"],
        }

        print(f"🔄 Processing source: {source['uuid']}")
        print(f"   Provider: {source['provider_uuid']}")
        print(f"   Version: {source['source_version_uuid']}")

        with pg_conn.cursor() as cur:
            cur.execute("""SELECT
                source_id,
                source_name,
                source_type,
                source_path,
                source_version,
                source_refresh,
                source_auth,
                source_headers,
                source_parameters,
                source_other
                FROM source WHERE source_uuid = %s""", (source['uuid'],))
            row = cur.fetchone()
            if row:
                source.update({
                    "id": row[0],
                    "name": row[1],
                    "type": row[2],
                    "path": row[3],
                    "version": row[4],
                    "refresh": row[5],
                    "auth": row[6],
                    "headers": row[7],
                    "parameters": row[8],
                })
                if isinstance(row[9], dict):
                    source.update(row[9])
                print(f"   🌐  Source path: {source['path']}")
                print(f"   🏷️  Source type: {source['type']}")
            else:
                print(f"   ⚠️ Source not found in DB.")
                return None

        handler_class = HANDLERS.get(source['type'].lower(), type(None))

        if not issubclass(handler_class, DataSourceType):
            print(f"   ❌ No handler for source_type '{source['type']}', skipping")
            return None

        handler = handler_class(source)

        try:
            file_bytes, content_type = handler.fetch()
        except Exception as e:
            print(f"   ❌ Fetch error: {e}")
            return None

        if 'application/rdf+xml' in content_type or 'application/xml' in content_type or 'text/xml' in content_type:
            file_extension = '.xml'
            file_format = 'xml'
        elif 'text/turtle' in content_type:
            file_extension = '.ttl'
            file_format = 'turtle'
        elif 'application/json' in content_type or 'application/ld+json' in content_type:
            file_extension = '.json'
            file_format = 'json-ld'
        else:
            file_extension = ''
            file_format = None

        base_folder = f"datalake/courses/{source['provider_uuid']}/{source['source_version_uuid']}/{source['uuid']}"
        manifest_path = f"{base_folder}/source_manifest.json"
        date_folder = f"{base_folder}/{date_str}"

        manifest_data = {"dates": [date_str], "latest_date": date_str}
        try:
            response = client.get_object(bucket_name, manifest_path)
            manifest_data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            if manifest_data["latest_date"] != date_str:
                if date_str not in manifest_data["dates"]:
                    manifest_data["dates"].append(date_str)
                manifest_data["latest_date"] = date_str
            print(f"   📄 Updated manifest: latest={date_str}")
        except Exception as e:
            if "NoSuchKey" not in str(e):
                raise e
            print(f"   📄 Creating new manifest")

        try:
            manifest_bytes = json.dumps(manifest_data, indent=4).encode('utf-8')
            client.put_object(
                bucket_name, manifest_path,
                BytesIO(manifest_bytes), length=len(manifest_bytes),
                content_type="application/json"
            )

            file_path = f"{date_folder}/{datetime_str}{file_extension}"
            client.put_object(
                bucket_name, file_path,
                BytesIO(file_bytes), length=len(file_bytes),
                content_type=content_type
            )
            print(f"   💾 Saved file to: {file_path}")

        except S3Error as e:
            print(f"❌ MinIO error: {e}")
            return None

        return {
            "provider_uuid": source['provider_uuid'],
            "source_version_uuid": source['source_version_uuid'],
            "source_uuid": source['uuid'],
            "source_type": source['type'],
            "file_path": file_path,
            "file_format": file_format,
            "content_type": content_type,
            "date": date_str,
        }
