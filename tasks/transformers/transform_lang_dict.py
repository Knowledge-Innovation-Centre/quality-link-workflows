from prefect import task

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, SKOS


@task(name="transform_lang_dict")
def transform_lang_dict(data, concept_scheme: str):

    scheme_uri = URIRef(concept_scheme)

    print(f"📥 Input: {len(data)} concepts in {scheme_uri}")

    g = Graph()

    g.bind("skos", SKOS)
    g.bind("rdf", RDF)

    for item in data:
        concept_uri = URIRef(item["concept_uri"])
        label = item["label_en"]

        g.add((concept_uri, RDF.type, SKOS.Concept))
        g.add((concept_uri, SKOS.prefLabel, Literal(label, lang='en')))
        g.add((concept_uri, SKOS.inScheme, scheme_uri))

    turtle_string = g.serialize(format='turtle')

    return turtle_string
