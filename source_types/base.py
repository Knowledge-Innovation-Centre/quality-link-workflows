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
        self._headers = {'user-agent': 'quality-link-aggregator/1.0.0-alpha'}
        if source['auth'] and source['auth'].get('type') == 'httpheader':
            self._headers[source['auth'].get('field', 'x-qualitylink-auth')] = source['auth'].get('value')
        if source['headers']:
            self._headers.update(source['headers'])

    def fetch(self):
        """
        Opens a session, delegates to _do_fetch(), and closes the session on exit.

        Returns (fetched data converted to RDF as bytes, MIME content-type).
        """
        with requests.Session() as session:
            session.headers.update(self._headers)
            return self._do_fetch(session)

    def _do_fetch(self, session):
        raise NotImplementedError
