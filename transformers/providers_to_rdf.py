if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import json
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, XSD, DCTERMS, FOAF, SKOS

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")
ROV = Namespace("http://www.w3.org/ns/regorg#")

@transformer
def providers_to_rdf(data, *args, **kwargs):
    """
    Convert DEQAR provider data to RDF graphs

    Args:
        data: output from the block fetching DEQAR data

    Returns:
        RDF graph
    """
    ttl = []
    counter = 0

    for provider in data[:5]:
        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)
        graph.bind("rov", ROV)
        deqar_to_rdf(provider, graph)
        ttl.append(graph.serialize(format='turtle'))
        del graph
        counter += 1

    print(f"Converted {counter} providers to RDF Turtle")
    return ttl


def deqar_to_rdf(provider_source, graph):
    """
    Inject JSON-LD context in a single provider record from DEQAR
    """
    provider = {
        '@context': {
            "adms": "http://www.w3.org/ns/adms#",
            "dcterms": "http://purl.org/dc/terms/",
            "elm": "http://data.europa.eu/snb/model/elm/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "ql": "http://data.quality-link.eu/ontology/v1#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "name_primary": { "@id":"skos:prefLabel", "@language":"en" },
            "identifiers": {
                "@id": "adms:identifier",
                "@container": "@set",
                "@context": {
                    "identifier": "skos:notation",
                    "resource":"elm:schemeName"
                }
            }
        },
        "@type": "ql:HigherEducationInstitution",
        "@id": f"https://data.deqar.eu/institution/{provider_source['id']}",
        **provider_source
    }

    if "identifiers" in provider and isinstance(provider['identifiers'], list):
        for i in provider['identifiers']:
            if i['resource'] == 'SCHAC':
                i['@type'] = "ql:SchacIdentifier"
                i['elm:schemeId'] = { "@id": "ql:Schac" }
            else:
                i['@type'] = "elm:Identifier"

    graph.parse(data=json.dumps(provider), format='json-ld')
    hei = URIRef(f"https://data.deqar.eu/institution/{provider_source['id']}")

    website = BNode()
    graph.add((hei, FOAF.homepage, website))
    graph.add((website, RDF.type, ELM.WebResource))
    graph.add((website, ELM.contentUrl, Literal(provider['website_link'])))

    deqar_id = BNode()
    graph.add((hei, ADMS.identifier, deqar_id))
    graph.add((deqar_id, RDF.type, ELM.Identifier))
    graph.add((deqar_id, SKOS.notation, Literal(provider['deqar_id'])))
    graph.add((deqar_id, ELM.schemeName, Literal("DEQARINST ID")))

    if provider['eter_id']:
        orgreg_id = BNode()
        graph.add((hei, ADMS.identifier, orgreg_id))
        graph.add((orgreg_id, RDF.type, QL.OrgRegIdentifier))
        graph.add((orgreg_id, SKOS.notation, Literal(provider['eter_id'])))
        graph.add((orgreg_id, ELM.schemeId, QL.OrgReg))
        graph.add((orgreg_id, ELM.schemeName, Literal("ETER ID")))

    for l in provider['locations']:
        location = BNode()
        address = BNode()
        #geo = BNode()
        graph.add((hei, ELM.location, location))
        graph.add((location, RDF.type, DCTERMS.Location))
        graph.add((location, ELM.address, address))
        #graph.add((location, LOCN.geometry, geo))
        graph.add((address, RDF.type, ELM.Address))
        graph.add((address, ELM.countryCode, URIRef(f'http://publications.europa.eu/resource/authority/country/{l["country"]["iso_3166_alpha3"]}')))

    for n in provider['names']:
        if not n['name_valid_to']:
            graph.add((hei, ROV.legalName, Literal(n['name_official'])))
        else:
            graph.add((hei, SKOS.altLabel, Literal(n['name_official'])))
            graph.add((hei, SKOS.altLabel, Literal(n['name_english'])))

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
