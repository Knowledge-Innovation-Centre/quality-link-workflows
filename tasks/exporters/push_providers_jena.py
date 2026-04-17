from prefect import task

import os
import requests


@task(name="push_providers_jena")
def push_providers_jena(data):
    fuseki_url = os.environ.get("FUSEKI_URL")
    fuseki_username = os.environ.get("FUSEKI_USERNAME")
    fuseki_password = os.environ.get("FUSEKI_PASSWORD")

    dataset_name = os.environ.get("FUSEKI_DATASET_NAME")
    update_url = f"{fuseki_url}/{dataset_name}/update"

    auth = None
    if fuseki_username and fuseki_password:
        auth = (fuseki_username, fuseki_password)

    print(f"🎯 Uploading to Fuseki dataset: {dataset_name}")
    print(f"{'='*60}")

    success_count = 0
    failed_count = 0

    for uri, rdfdata in data:
        try:
            sparql = f"""
                WITH <http://data.quality-link.eu/graph/reference>
                DELETE {{
                  ?root ?p0 ?o0 .
                  ?bn1 ?p1 ?o1 .
                  ?bn2 ?p2 ?o2 .
                  ?bn3 ?p3 ?o3 .
                }}
                WHERE {{
                  VALUES ?root {{ <{uri}> }}
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
                  GRAPH <http://data.quality-link.eu/graph/reference> {{
                    {rdfdata}
                  }}
                }}
            """

            upload_response = requests.post(update_url, data=sparql,
                              headers={"Content-Type": "application/sparql-update"},
                              auth=auth, timeout=60)

            if upload_response.status_code in (200, 204):
                success_count += 1
            else:
                print(f"   ❌ Fuseki upload failed: {upload_response.status_code}")
                print(f"      Response: {upload_response.text[:200]}")
                failed_count += 1

        except requests.RequestException as e:
            print(f"   ❌ Request error uploading to Fuseki: {e}")
            failed_count += 1
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error during upload: {e}")
            failed_count += 1
            continue

    print(f"✅ {success_count} records successfully uploaded to Fuseki")
    print(f"❌ {failed_count} errors during upload")

    return {
        "success": success_count,
        "failed": failed_count,
    }
