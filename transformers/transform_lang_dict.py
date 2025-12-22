if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, SKOS


@transformer
def transform(data, *args, **kwargs):
    
    print(f"📥 Input: {len(data)} language entries")
    
    g = Graph()
    
    g.bind("skos", SKOS)
    g.bind("rdf", RDF)
    
    for item in data:
        language_uri = URIRef(item["language_uri"])
        label = item["label_en"]
        
        g.add((language_uri, RDF.type, SKOS.Concept))
        g.add((language_uri, SKOS.prefLabel, Literal(label, lang='en')))
    
    turtle_string = g.serialize(format='turtle')
    
    # print(f"📦 Serialized to {len(turtle_string)} bytes")
    # print("\n🔍 Sample output (first 10 lines):")
    # print('\n'.join(turtle_string.split('\n')[:10]))
    
    return turtle_string  


# @test
# def test_output(output, *args) -> None:
#     assert output is not None, 'The output is undefined'
#     assert isinstance(output, str), 'Output should be a string'
#     assert len(output) > 0, 'Output should not be empty'
#     assert '@prefix' in output, 'Output should contain Turtle prefixes'
#     print(f"✅ Test passed: {len(output)} bytes of Turtle data")