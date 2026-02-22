import os
from typing import Dict, List
from minio import Minio
from minio.error import S3Error
import requests
import psycopg2
from datetime import datetime, timezone
import uuid
from rdflib import Graph, Namespace, Literal, URIRef, BNode, RDF
from rdflib.namespace import XSD, DCTERMS, OWL

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")


def has_type(graph, subject, *types):
    return any((subject, RDF.type, t) in graph for t in types)


def _collect(src: Graph, dst: Graph, node, visited: set):
    if node in visited:
        return
    visited.add(node)
    for p, o in src.predicate_objects(node):
        dst.add((node, p, o))
        if isinstance(o, BNode):
            _collect(src, dst, o, visited)


def extract_subgraph(graph: Graph, root: URIRef) -> Graph:
    """Return a new Graph with root's triples and all reachable blank-node descendants."""
    sub = Graph()
    _collect(graph, sub, root, set())
    return sub


def enrich_rdf_graph(file_content: bytes, file_format: str, provider_uuid: str, provider_uri: str) -> tuple:
    """
    Enrich RDF graph with ingestion metadata and course UUIDs.
    Returns (enriched_bytes, course_uuids_list).
    course_uuids is local to this call, not module-level, to prevent stale state across runs.
    """
    course_uuids = set()

    try:
        graph = Graph()
        graph.parse(data=file_content, format=file_format)

        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)
        graph.bind("owl", OWL)

        current_datetime = datetime.now(timezone.utc)
        current_date = current_datetime.date()

        subjects_processed = 0
        hei_count = 0
        los_count = 0
        los_with_publisher = 0
        los_publisher_injected = 0
        los_publisher_from_loi = 0
        loi_count = 0
        loi_with_provided_by = 0
        loi_provider_injected = 0
        loi_with_course_link = 0

        owl_same_as_triples = []
        loi_subjects = []
        los_subjects = []

        # Pass 1: metadata timestamps + UUID generation + collect typed subjects
        for subject in graph.subjects(unique=True):
            if not isinstance(subject, URIRef):
                continue

            subjects_processed += 1

            graph.add((subject, QL.ingestedDate, Literal(current_date, datatype=XSD.date)))
            graph.add((subject, QL.ingestedAt, Literal(current_datetime, datatype=XSD.dateTime)))

            course_uuid = None

            if has_type(graph, subject, QL.HigherEducationInstitution, ELM.Organisation):
                hei_count += 1
                # for now, do nothing - TO DO: try to match with provider list and add OWL.sameAs

            elif has_type(graph, subject, QL.LearningOpportunitySpecification,
                          ELM.Qualification, ELM.LearningAchievementSpecification):
                los_count += 1
                course_uuid = str(uuid.uuid5(uuid.UUID(provider_uuid), str(subject)))
                owl_same_as_triples.append((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, subject))
                los_subjects.append(subject)
                if (subject, DCTERMS.publisher, None) in graph:
                    los_with_publisher += 1

            elif has_type(graph, subject, QL.LearningOpportunityInstance, ELM.LearningOpportunity):
                loi_count += 1
                los_uri = graph.value(subject, ELM.learningAchievementSpecification)
                if los_uri and isinstance(los_uri, URIRef):
                    loi_with_course_link += 1
                loi_subjects.append(subject)
                if (subject, ELM.providedBy, None) in graph:
                    loi_with_provided_by += 1

            if course_uuid is not None:
                course_uuids.add(course_uuid)

        for s, p, o in owl_same_as_triples:
            graph.add((s, p, o))

        # Pass 2: inject elm:providedBy on LOIs that are missing it
        for loi in loi_subjects:
            if (loi, ELM.providedBy, None) not in graph and provider_uri:
                graph.add((loi, ELM.providedBy, URIRef(provider_uri)))
                loi_provider_injected += 1

        # Pass 3: inject dcterms:publisher on LOSes from linked LOI providers, or fall back to provider_uri
        for los_uri in los_subjects:
            if (los_uri, DCTERMS.publisher, None) in graph:
                continue
            loi_providers = set()
            for loi in graph.subjects(ELM.learningAchievementSpecification, los_uri):
                for p in graph.objects(loi, ELM.providedBy):
                    loi_providers.add(p)
            if loi_providers:
                for p in loi_providers:
                    graph.add((los_uri, DCTERMS.publisher, p))
                los_publisher_from_loi += 1
            elif provider_uri:
                graph.add((los_uri, DCTERMS.publisher, URIRef(provider_uri)))
                los_publisher_injected += 1

        enriched_content = graph.serialize(format=file_format, encoding='utf-8')

        print(f"   📊 Enrichment stats:")
        print(f"      - Total subjects: {subjects_processed}")
        print(f"      - HEI: {hei_count}")
        print(f"      - LOS: {los_count} ({los_with_publisher} with publisher, "
              f"{los_publisher_from_loi} from LOI, {los_publisher_injected} fallback injected)")
        print(f"      - LOI: {loi_count} ({loi_with_provided_by} with providedBy, "
              f"{loi_provider_injected} injected, {loi_with_course_link} with course link)")
        print(f"      - Course UUIDs: {len(course_uuids)}, Total triples: {len(graph)}")

        return enriched_content, list(course_uuids), graph

    except Exception as e:
        print(f"   ⚠️ RDF enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return file_content, [], None


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

    provider_uri = None
    try:
        pg_conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB_NAME"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD")
        )
        with pg_conn.cursor() as cur:
            cur.execute("SELECT base_id FROM provider WHERE provider_uuid = %s", (provider_uuid,))
            row = cur.fetchone()
            if row:
                provider_uri = f"https://data.deqar.eu/institution/{row[0]}"
        pg_conn.close()
        if provider_uri:
            print(f"   🏛️ Provider URI: {provider_uri}")
        else:
            print(f"   ⚠️ Could not resolve provider URI for {provider_uuid}")
    except Exception as e:
        print(f"   ⚠️ Provider URI lookup failed: {e}")

    print(f"   🔧 Enriching RDF content...")
    enriched_content, course_uuids, enriched_graph = enrich_rdf_graph(file_content, file_format, provider_uuid, provider_uri)
    print(f"   ✅ Enriched: {len(course_uuids)} course UUIDs found")

    fuseki_url = os.environ.get("FUSEKI_URL")
    fuseki_username = os.environ.get("FUSEKI_USERNAME")
    fuseki_password = os.environ.get("FUSEKI_PASSWORD")
    dataset_name = os.environ.get("FUSEKI_DATASET_NAME")
    auth = (fuseki_username, fuseki_password) if fuseki_username and fuseki_password else None

    if enriched_graph is None:
        print("❌ Skipping Fuseki upload — enrichment failed")
        return None

    named_uris = [s for s in enriched_graph.subjects(unique=True) if isinstance(s, URIRef)]
    update_url = f"{fuseki_url}/{dataset_name}/update"
    failed = 0

    for uri in named_uris:
        uri_str = str(uri)
        subgraph_nt = extract_subgraph(enriched_graph, uri).serialize(format='nt')

        sparql = f"""WITH <http://data.quality-link.eu/graph/courses>
            DELETE {{
              ?root ?p0 ?o0 .
              ?bn1 ?p1 ?o1 .
              ?bn2 ?p2 ?o2 .
              ?bn3 ?p3 ?o3 .
            }}
            WHERE {{
              VALUES ?root {{ <{uri_str}> }}
              # Level 0: direct statements about root
              ?root ?p0 ?o0 .
              # Level 1: blank nodes directly under root
              OPTIONAL {{
                ?root ?px0 ?bn1 .
                FILTER(isBlank(?bn1))
                ?bn1 ?p1 ?o1 .
                # Level 2: blank nodes under level-1 blank nodes
                OPTIONAL {{
                  ?bn1 ?px1 ?bn2 .
                  FILTER(isBlank(?bn2))
                  ?bn2 ?p2 ?o2 .
                  # Level 3: blank nodes under level-2 blank nodes
                  OPTIONAL {{
                    ?bn2 ?px2 ?bn3 .
                    FILTER(isBlank(?bn3))
                    ?bn3 ?p3 ?o3 .
                  }}
                }}
              }}
            }} ;
            INSERT DATA {{
              GRAPH <http://data.quality-link.eu/graph/courses> {{
              {subgraph_nt}
              }}
            }}
        """

        r = requests.post(update_url, data=sparql,
                          headers={"Content-Type": "application/sparql-update"},
                          auth=auth, timeout=60)
        if r.status_code not in (200, 204):
            print(f"   ❌ SPARQL Update failed for {uri_str}: {r.status_code}")
            failed += 1

    if failed:
        print(f"   ⚠️ {failed}/{len(named_uris)} subjects failed to update in Fuseki ({dataset_name})")
    else:
        print(f"   ✅ Pushed {len(named_uris)} subjects to Fuseki ({dataset_name})")

    del enriched_graph

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
