from ql.source_types.base import DataSourceType

import os
import requests
from urllib.parse import urlparse, urljoin

from rdflib import Graph, Namespace, Literal, URIRef, RDF
from rdflib.namespace import XSD, DCTERMS
from datetime import datetime, timezone
import uuid
from typing import Dict, List, Any

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")

class OoapiDataSource(DataSourceType):
    """
    OOAPI (v5) data source
    """

    def fetch(self):
        """
        Fetch an ELM file via HTTP GET. Returns (content_bytes, content_type).
        """

        url = urljoin(self.source['path'], 'courses')

        print(f"   🔽 Making OOAPI request to: {url}")

        params = {}
        if self.source['parameters']:
            params.update(self.source['parameters'])

        params['pageSize'] = self.source.get('pageSize', 250)
        params['pageNumber'] = 0

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        course_uuids = []
        success_count = 0
        failed_count = 0

        has_next_page = True

        while has_next_page:
            params['pageNumber'] += 1
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            items = data.get('items', [])
            page = data.get('pageNumber', 1)
            page_courses = len(items)
            has_next_page = data.get('hasNextPage', False)

            print(f"🔄 Processing page {page} with {page_courses} courses")

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

        courseId = course.get('courseId')
        if not courseId:
            print(f"   ⚠️ Skipping course without courseId")
            return None

        provider_uri = URIRef(f"http://data.quality-link.eu/providers/{self.source['provider_uuid']}")
        course_uri = URIRef(f"http://data.quality-link.eu/providers/{self.source['provider_uuid']}/courses/{courseId}")
        course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(course_uri)))

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))

        graph.add((course_uri, QL.course_uuid, Literal(course_uuid)))
        graph.add((course_uri, QL.provider_uuid, Literal(self.source['provider_uuid'])))

        current_datetime = datetime.now(timezone.utc)
        current_date = current_datetime.date()
        graph.add((course_uri, QL.ingestedDate, Literal(current_date, datatype=XSD.date)))
        graph.add((course_uri, QL.ingestedAt, Literal(current_datetime, datatype=XSD.dateTime)))

        if course.get('primaryCode'):
            graph.add((course_uri, DCTERMS.identifier, Literal(course.get('primaryCode'))))

        if course.get('name'):
            title = self.extract_english_value(course.get('name'))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang='en')))

        if course.get('description'):
            description = self.extract_english_value(course.get('description'))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang='en')))

        if course.get('learningOutcomes'):
            outcomes = self.extract_english_value(course.get('learningOutcomes'))
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

        graph.add((course_uri, DCTERMS.publisher, provider_uri))

        return course_uuid

