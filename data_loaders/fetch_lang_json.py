if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests


@data_loader
def load_data(*args, **kwargs):

    url = "https://publications.europa.eu/webapi/rdf/sparql?default-graph-uri=&query=PREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0A%0D%0ASELECT+distinct+%3Flanguage_uri+%3Flabel_en%0D%0A%0D%0AFROM++%3Chttp%3A%2F%2Fpublications.europa.eu%2Fresource%2Fauthority%2Flanguage%3E%0D%0A%0D%0AWHERE+%7B%0D%0A++++%3Flanguage_uri+a+skos%3AConcept+.%0D%0A++++%3Flanguage_uri+skos%3AprefLabel+%3Flabel_en+.%0D%0A++++filter%28lang%28%3Flabel_en%29+%3D+%22en%22%29%0D%0A%7D&format=application%2Fsparql-results%2Bjson&timeout=0&debug=on&run=+Run+Query+"
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        bindings = data.get("results", {}).get("bindings", [])
        
        print(f"✅ Retrieved {len(bindings)} language entries")
        
        languages = []
        for binding in bindings:
            language_uri = binding.get("language_uri", {}).get("value")
            label_en = binding.get("label_en", {}).get("value")
            
            if language_uri and label_en:
                languages.append({
                    "language_uri": language_uri,
                    "label_en": label_en
                })
        
        return languages
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"❌ Error parsing response: {e}")
        raise


# @test
# def test_output(output, *args) -> None:

#     assert output is not None, 'The output is undefined'
#     assert isinstance(output, list), 'Output should be a list'
#     assert len(output) > 0, 'Output list is empty'
    
#     first_item = output[0]
#     assert 'language_uri' in first_item, 'Missing language_uri key'
#     assert 'label_en' in first_item, 'Missing label_en key'
#     assert first_item['language_uri'].startswith('http'), 'language_uri should be a URI'
#     assert len(first_item['label_en']) > 0, 'label_en should not be empty'
    
#     print(f"✅ Test passed: {len(output)} language records loaded")