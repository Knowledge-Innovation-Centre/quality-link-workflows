import os
from typing import Dict, List
from minio import Minio
from minio.error import S3Error
import requests
import psycopg2
from datetime import datetime, timezone
import uuid
from rdflib import Graph, Namespace, Literal, URIRef, RDF
from rdflib.namespace import XSD, DCTERMS

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")


def enrich_rdf_graph(file_content: bytes, file_format: str, provider_uuid: str) -> tuple:
    """
    Enrich RDF graph with ingestion metadata and course UUIDs.
    Returns (enriched_bytes, course_uuids_list).
    course_uuids is local to this call, not module-level, to prevent stale state across runs.
    """
    course_uuids = []

    try:
        graph = Graph()
        graph.parse(data=file_content, format=file_format)

        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        current_datetime = datetime.now(timezone.utc)
        current_date = current_datetime.date()

        subjects_processed = 0
        hei_count = 0
        los_count = 0
        los_with_publisher = 0
        loi_count = 0
        loi_with_provided_by = 0
        loi_with_course_link = 0

        for subject in graph.subjects(unique=True):
            if not isinstance(subject, URIRef):
                continue

            subjects_processed += 1

            graph.add((subject, QL.ingestedDate, Literal(current_date, datatype=XSD.date)))
            graph.add((subject, QL.ingestedAt, Literal(current_datetime, datatype=XSD.dateTime)))

            course_uuid = None

            if (subject, RDF.type, QL.HigherEducationInstitution) in graph:
                hei_count += 1
                graph.add((subject, QL.provider_uuid, Literal(provider_uuid)))

            elif (subject, RDF.type, QL.LearningOpportunitySpecification) in graph:
                los_count += 1
                course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(subject)))
                graph.add((subject, QL.course_uuid, Literal(course_uuid)))
                if (subject, DCTERMS.publisher, None) in graph:
                    graph.add((subject, QL.provider_uuid, Literal(provider_uuid)))
                    los_with_publisher += 1

            elif (subject, RDF.type, QL.LearningOpportunityInstance) in graph:
                loi_count += 1
                los_uri = graph.value(subject, ELM.learningAchievementSpecification)
                if los_uri and isinstance(los_uri, URIRef):
                    course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(los_uri)))
                    graph.add((subject, QL.course_uuid, Literal(course_uuid)))
                    loi_with_course_link += 1
                if (subject, ELM.providedBy, None) in graph:
                    graph.add((subject, QL.provider_uuid, Literal(provider_uuid)))
                    loi_with_provided_by += 1

            if course_uuid is not None and course_uuid not in course_uuids:
                course_uuids.append(course_uuid)

        enriched_content = graph.serialize(format=file_format, encoding='utf-8')

        print(f"   📊 Enrichment stats:")
        print(f"      - Total subjects: {subjects_processed}")
        print(f"      - HEI: {hei_count}, LOS: {los_count} ({los_with_publisher} with publisher)")
        print(f"      - LOI: {loi_count} ({loi_with_provided_by} with providedBy, {loi_with_course_link} with course link)")
        print(f"      - Course UUIDs: {len(course_uuids)}, Total triples: {len(graph)}")

        return enriched_content, course_uuids

    except Exception as e:
        print(f"   ⚠️ RDF enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return file_content, []


@transformer
def transform(messages: List[Dict], *args, **kwargs):

    message = messages[0] if messages else None
    if message is None:
        print("⚠️ No message received")
        return None

    provider_uuid = message.get("provider_uuid")
    source_version_uuid = message.get("source_version_uuid")
    source_uuid = message.get("source_uuid")
    source_type = message.get("source_type", "unknown")
    file_path = message.get("file_path")
    file_format = message.get("file_format", "turtle")
    content_type = message.get("content_type", "text/turtle")

    print(f"🔄 Enriching source: {source_uuid}")
    print(f"   Provider: {provider_uuid}, Format: {file_format}")
    print(f"   MinIO path: {file_path}")

    if not file_path:
        print("❌ No file_path in message")
        return None

    try:
        minio_client = Minio(
            os.environ.get("MINIO_HOST"),
            access_key=os.environ.get("MINIO_ROOT_USER"),
            secret_key=os.environ.get("MINIO_ROOT_PASSWORD"),
            secure=False
        )
        bucket_name = os.environ.get("MINIO_BUCKET_NAME")
        print("✅ Connected to MinIO")
    except Exception as e:
        print(f"❌ MinIO connection error: {e}")
        return None

    try:
        response = minio_client.get_object(bucket_name, file_path)
        file_content = response.read()
        response.close()
        response.release_conn()
        print(f"   📥 Downloaded from MinIO ({len(file_content)} bytes)")
    except Exception as e:
        print(f"❌ Error downloading from MinIO: {e}")
        return None

    print(f"   🔧 Enriching RDF content...")
    enriched_content, course_uuids = enrich_rdf_graph(file_content, file_format, provider_uuid)
    print(f"   ✅ Enriched: {len(course_uuids)} course UUIDs found")

    fuseki_url = os.environ.get("FUSEKI_URL")
    fuseki_username = os.environ.get("FUSEKI_USERNAME")
    fuseki_password = os.environ.get("FUSEKI_PASSWORD")
    dataset_name = os.environ.get("FUSEKI_DATASET_NAME")
    upload_url = f"{fuseki_url}/{dataset_name}/data"
    auth = (fuseki_username, fuseki_password) if fuseki_username and fuseki_password else None

    try:
        upload_response = requests.post(
            upload_url,
            data=enriched_content,
            headers={"Content-Type": content_type},
            auth=auth,
            timeout=60
        )
        if upload_response.status_code == 200:
            print(f"   ✅ Uploaded to Jena Fuseki ({dataset_name})")
        else:
            print(f"   ❌ Fuseki upload failed: {upload_response.status_code}")
            print(f"      {upload_response.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ Fuseki upload error: {e}")
        return None

    try:
        pg_conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB_NAME"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        with pg_conn.cursor() as cur:
            filename = os.path.basename(file_path)
            current_time = datetime.now()
            cur.execute(
                """
                UPDATE source
                SET last_file_pushed = %s,
                    last_file_pushed_date = %s,
                    last_file_pushed_path = %s,
                    updated_at = %s
                WHERE source_uuid = %s
                """,
                (filename, current_time, file_path, current_time, source_uuid)
            )
        pg_conn.commit()
        pg_conn.close()
        print(f"   💾 Updated source record in PostgreSQL")
    except Exception as e:
        print(f"   ⚠️ DB update failed: {e}")

    return {
        "provider_uuid": provider_uuid,
        "source_version_uuid": source_version_uuid,
        "source_type": source_type,
        "course_uuids": course_uuids,
    }
