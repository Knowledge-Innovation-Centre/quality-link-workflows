from prefect import task

import requests


@task(name="fetch_lang_json")
def fetch_lang_json(concept_scheme: str):

    query = f"""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT distinct ?concept_uri ?label_en

        FROM <{concept_scheme}>

        WHERE {{
            ?concept_uri a skos:Concept .
            ?concept_uri skos:prefLabel ?label_en .
            filter(lang(?label_en) = "en")
        }}
    """

    try:
        response = requests.get("https://publications.europa.eu/webapi/rdf/sparql", timeout=60, params={
            'query': query,
            'format': 'application/sparql-results+json',
            'timeout': '0',
        })
        response.raise_for_status()

        data = response.json()

        bindings = data.get("results", {}).get("bindings", [])

        print(f"✅ Retrieved {len(bindings)} controlled vocabulary labels for {concept_scheme}")

        concepts = []
        for binding in bindings:
            concept_uri = binding.get("concept_uri", {}).get("value")
            label_en = binding.get("label_en", {}).get("value")

            if concept_uri and label_en:
                concepts.append({
                    "concept_uri": concept_uri,
                    "label_en": label_en
                })

        return concepts

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"❌ Error parsing response: {e}")
        raise
