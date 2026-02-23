if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests


@data_loader
def load_data(*args, **kwargs):

    scheme_uri = kwargs.get("CONCEPT_SCHEME")

    query = f"""PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT distinct ?concept_uri ?label_en

        FROM <{scheme_uri}>

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
        
        print(f"✅ Retrieved {len(bindings)} controlled vocabulary labels for {scheme_uri}")
        
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


@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
    
    assert 'concept_uri' in output, 'Missing concept_uri key'
    assert 'label_en' in output, 'Missing label_en key'
    assert output['concept_uri'].startswith('http'), 'concept_uri should be a URI'
    assert len(output['label_en']) > 0, 'label_en should not be empty'
