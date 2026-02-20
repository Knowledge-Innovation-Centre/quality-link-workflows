from typing import Dict

import requests

class DataSourceType:
    """
    Base class for all data sources

    This encapsulates some generic parameters, e.g. header parameters

    Sub-class this for different data source types
    """

    OK_TYPES = (
        'application/rdf+xml',
        'application/xml',
        'text/xml',
        'text/turtle',
        'application/json',
        'application/ld+json',
    )

    def __init__(self, source: Dict):
        self.source = source
        # requests session keeps header parameters
        self.session = requests.Session()
        if source['auth'] and source['auth'].get('type') == 'httpheader':
            self.session.headers.update({ source['auth'].get('field', 'x-qualitylink-auth'): source['auth'].get('value') })
        if source['headers']:
            self.session.headers.update(source['headers'])
        self.session.headers['user-agent'] = 'quality-link-aggregator/1.0.0-alpha'


    def fetch(self):
        """
        This method should fetch data and return a tuple of:

        (fetched data converted to RDF as bytes, MIME content-type)
        """
        raise NotImplemented

