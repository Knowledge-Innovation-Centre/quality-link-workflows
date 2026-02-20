from ql.source_types.base import DataSourceType

import os
import requests
from urllib.parse import urlparse

class ElmDataSource(DataSourceType):

    def fetch(self):
    #(source_path: str, **kwargs):
        """
        Fetch an ELM file via HTTP GET. Returns (content_bytes, content_type).
        """
        print(f"   🔽 Downloading ELM file from: {self.source['path']}")

        response = requests.get(self.source['path'], timeout=60)
        response.raise_for_status()

        file_extension = os.path.splitext(urlparse(self.source['path']).path)[1]
        content_type = response.headers.get('content-type')

        if 'contentType' in self.source and self.source['contentType'] in self.OK_TYPES:
            # source definition defines fixed content-type
            if self.source['contentType'] != content_type:
                print(f"   ⚠️ Source config overwrites actual content-type '{content_type}' to '{self.source['contentType']}'.")
            return response.content, self.source['contentType']

        if content_type not in self.OK_TYPES:
            print(f"   ⚠️ Unsupported content-type '{content_type}', trying to guess from file extension.")
            if file_extension in [ '.rdf', '.xml' ]:
                content_type = 'application/rdf+xml'
            elif file_extension in [ '.json', '.jsonld' ]:
                content_type = 'application/ld+json'
            elif file_extension == '.ttl':
                content_type = 'text/turtle'

            if content_type in self.OK_TYPES:
                print(f"   ⚠️ Guessed '{content_type}'.")
            else:
                print(f"   ❌ Unknown extension, keeping content-type.")

        return response.content, content_type

