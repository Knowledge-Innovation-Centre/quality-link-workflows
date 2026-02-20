from typing import Dict

class DataSourceType:
    """
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

    def fetch(self):
        """
        This method should fetch data and return a tuple of:

        (fetched data converted to RDF as bytes, MIME content-type)
        """
        raise NotImplemented

