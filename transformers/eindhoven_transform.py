if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from rdflib import Graph, Namespace, Literal, URIRef, RDF
from rdflib.namespace import XSD, DCTERMS
from datetime import datetime, timezone
import uuid
from typing import Dict, List, Any


QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
BASE_NAMESPACE = "https://apigateway-ota.osiris-link.nl/api/tue/acc/ords/ooapi/v5/"

PROVIDER_UUID = "37817e5f-965c-4512-ae43-10f5c4eb2072"


def extract_english_value(multilingual_field: Any) -> str:

    if isinstance(multilingual_field, str):
        return multilingual_field
    
    if not isinstance(multilingual_field, list) or not multilingual_field:
        return ""
    
    for item in multilingual_field:
        if isinstance(item, dict):
            lang = item.get('language', '').lower()
            if 'en' in lang:  
                return item.get('value', '')
    
    if isinstance(multilingual_field[0], dict):
        return multilingual_field[0].get('value', '')
    
    return str(multilingual_field[0])


def map_course_to_rdf(course: Dict, graph: Graph) -> str:

    courseId = course.get('courseId')
    if not courseId:
        print(f"   ⚠️ Skipping course without courseId")
        return None
    
    course_uri = URIRef(f"{BASE_NAMESPACE}courses/{courseId}")
    course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(course_uri)))
    
    graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
    
    graph.add((course_uri, QL.course_uuid, Literal(course_uuid)))
    graph.add((course_uri, QL.provider_uuid, Literal(PROVIDER_UUID)))
    
    current_datetime = datetime.now(timezone.utc)
    current_date = current_datetime.date()
    graph.add((course_uri, QL.ingestedDate, Literal(current_date, datatype=XSD.date)))
    graph.add((course_uri, QL.ingestedAt, Literal(current_datetime, datatype=XSD.dateTime)))
    
    if course.get('primaryCode'):
        graph.add((course_uri, DCTERMS.identifier, Literal(course.get('primaryCode'))))
    
    if course.get('name'):
        title = extract_english_value(course.get('name'))
        if title:
            graph.add((course_uri, DCTERMS.title, Literal(title, lang='en')))
    
    if course.get('description'):
        description = extract_english_value(course.get('description'))
        if description:
            graph.add((course_uri, DCTERMS.description, Literal(description, lang='en')))
    
    if course.get('learningOutcomes'):
        outcomes = extract_english_value(course.get('learningOutcomes'))
        if outcomes:
            graph.add((course_uri, ELM.learningOutcomeDescription, Literal(outcomes, lang='en')))
    
    if course.get('studyLoad'):
        study_load = course['studyLoad']
        if isinstance(study_load, dict) and study_load.get('value'):
            graph.add((course_uri, ELM.volumeOfLearning, Literal(study_load['value'], datatype=XSD.decimal)))
    
    if course.get('level'):
        graph.add((course_uri, ELM.ISCEDFCode, Literal(course.get('level'))))
    
    if course.get('teachingLanguage'):
        lang_code = course.get('teachingLanguage')
        if isinstance(lang_code, str):
            graph.add((course_uri, DCTERMS.language, Literal(lang_code)))
    
    if course.get('fieldsOfStudy'):
        fields = course.get('fieldsOfStudy')
        if isinstance(fields, list):
            for field in fields:
                if isinstance(field, str):
                    graph.add((course_uri, ELM.thematicArea, Literal(field)))
    
    if course.get('link'):
        graph.add((course_uri, ELM.homePage, URIRef(course.get('link'))))
    
    provider_uri = URIRef(f"http://data.quality-link.eu/providers/{PROVIDER_UUID}")
    graph.add((course_uri, DCTERMS.publisher, provider_uri))
    
    return course_uuid


@transformer
def transform(data, *args, **kwargs) -> Dict[str, Any]:

    items = data.get('items', [])
    total_courses = len(items)
    
    print(f"🔄 Transforming {total_courses} courses to RDF")
    print(f"{'='*60}")
    
    graph = Graph()
    graph.bind("ql", QL)
    graph.bind("elm", ELM)
    graph.bind("dcterms", DCTERMS)
    
    course_uuids = []
    success_count = 0
    failed_count = 0
    
    for idx, course in enumerate(items, 1):
        try:
            course_uuid = map_course_to_rdf(course, graph)
            
            if course_uuid:
                course_uuids.append(course_uuid)
                success_count += 1
                
                if idx % 10 == 0 or idx == total_courses:
                    print(f"   ✅ Processed {idx}/{total_courses} courses")
            else:
                failed_count += 1
                
        except Exception as e:
            print(f"   ❌ Failed to process course {idx}: {e}")
            failed_count += 1
            continue
    
    print(f"\n{'='*60}")
    print(f"📊 TRANSFORMATION SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successfully transformed: {success_count}")
    print(f"❌ Failed:                   {failed_count}")
    print(f"📈 Total courses:            {total_courses}")
    print(f"🔢 Total RDF triples:        {len(graph)}")
    print(f"🆔 Unique course UUIDs:      {len(course_uuids)}")
    print(f"{'='*60}")
    
    rdf_content = graph.serialize(format='turtle', encoding='utf-8')
    
    kwargs['rdf_content'] = rdf_content
    kwargs['course_uuids'] = course_uuids
    
    return {
        "success": success_count,
        "failed": failed_count,
        "total": total_courses,
        "triples_created": len(graph),
        "course_uuids": course_uuids,
        "rdf_content": rdf_content  
    }


@test
def test_output(output, *args) -> None:

    assert output is not None, 'Output is undefined'
    assert isinstance(output, dict), 'Output should be a dictionary'
    assert 'success' in output, 'Output should contain success count'
    assert 'course_uuids' in output, 'Output should contain course_uuids'
    assert 'rdf_content' in output, 'Output should contain rdf_content'
    assert len(output['course_uuids']) == output['success'], 'UUIDs should match success count'
    print(f"✅ Test passed: {output['success']} courses transformed to {output['triples_created']} triples")