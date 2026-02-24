from ql.source_types.base import DataSourceType

import os
import requests
from urllib.parse import urlparse, urljoin

from rdflib import Graph, Namespace, Literal, URIRef, BNode, RDF
from rdflib.namespace import XSD, DCTERMS, OWL, SKOS, FOAF
import uuid
from typing import Dict, List, Any

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")

class EduApiDataSource(DataSourceType):
    """
    Edu-API (v1) data source
    """

    LEVEL_MAP = {
        "undergraduate": URIRef("http://data.europa.eu/snb/eqf/6"),
        "graduate": URIRef("http://data.europa.eu/snb/eqf/7"),
        "doctoral": URIRef("http://data.europa.eu/snb/eqf/8"),
    }

    def _do_fetch(self, session):
        """
        Fetch Edu-API courses via paginated HTTP GET. Returns (content_bytes, content_type).
        """

        url = urljoin(self.source['path'], 'courseTemplates')

        print(f"   🔽 Making Edu-API request to: {url}")

        params = {}
        if self.source['parameters']:
            params.update(self.source['parameters'])

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        course_uuids = []
        success_count = 0
        failed_count = 0

        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        items = response.json()
        page_courses = len(items)

        print(f"🔄 Processing page with {page_courses} courses")

        for course in items:
            course_uuid = self.map_course_to_rdf(course, graph)
            if course_uuid:
                course_uuids.append(course_uuid)
                success_count += 1
            else:
                failed_count += 1

        print(f"✅ Successfully transformed: {success_count}")
        print(f"❌ Failed:                   {failed_count}")
        print(f"🔢 Total RDF triples:        {len(graph)}")
        print(f"🆔 Unique course UUIDs:      {len(course_uuids)}")

        rdf_content = graph.serialize(format='turtle', encoding='utf-8')

        return (rdf_content, 'text/turtle')


    def extract_english_value(self, multilingual_field: Any) -> str:

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


    def map_course_to_rdf(self, course: Dict, graph: Graph) -> str:

        courseId = course.get('sourcedId')
        if not courseId:
            print(f"   ⚠️ Skipping course without courseId")
            return None

        course_uri = URIRef(f"http://data.quality-link.eu/providers/{self.source['provider_uuid']}/courses/{courseId}")
        course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(course_uri)))

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))

        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        if course.get('primaryCode') and isinstance(course['primaryCode'], dict):
            code = BNode()
            graph.add((code, RDF.type, ELM.Identifier))
            graph.add((code, SKOS.notation, Literal(course['primaryCode'].get('identifier'))))
            graph.add((code, ELM.schemeName, Literal(course['primaryCode'].get('identifierType'))))
            graph.add((course_uri, ADMS.identifier, code))

        if course.get('title'):
            title = self.extract_english_value(course.get('title'))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang='en')))

        if course.get('description'):
            description = self.extract_english_value(course.get('description'))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang='en')))

        if course.get('level'):
            if course['level'] in self.LEVEL_MAP:
                graph.add((course_uri, ELM.EQFLevel, self.LEVEL_MAP[course['level']]))

        """
        if course.get('teachingLanguage'):
            lang_code = course.get('teachingLanguage')
            if isinstance(lang_code, str):
                graph.add((course_uri, DCTERMS.language, URIRef(f"http://publications.europa.eu/resource/authority/language/{lang_code.upper()}")))
        """

        return course_uuid

