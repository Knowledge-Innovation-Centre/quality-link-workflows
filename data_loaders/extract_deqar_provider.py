if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import requests
import json
import re
from datetime import datetime
import time
import uuid
import pandas as pd
from io import StringIO

@data_loader
def load_data(*args, **kwargs):

    API_BASE_URL = kwargs.get('API_BASE_URL', 'https://backend.testzone.eqar.eu/connectapi/v1/providers/')
    LIMIT = kwargs.get('LIMIT', 2000)
    INITIAL_OFFSET = kwargs.get('OFFSET', 0)
    MAX_RETRIES = kwargs.get('MAX_RETRIES', 3)
    RETRY_DELAY = kwargs.get('RETRY_DELAY', 10)
    REQUEST_DELAY = kwargs.get('REQUEST_DELAY', 1)

    total_count = 0
    failed_pages = []
    
    print(f"🚀 Starting data collection from {API_BASE_URL}")
    print(f"📊 Using pagination with LIMIT={LIMIT}, starting at OFFSET={INITIAL_OFFSET}")
    
    offset = INITIAL_OFFSET
    more_pages = True
    
    results = []

    while more_pages:
        print(f"🔍 Fetching: {offset}-{offset+LIMIT}")
        
        retry_count = 0
        request_successful = False
        
        while retry_count <= MAX_RETRIES and not request_successful:
            try:
                if retry_count > 0:
                    print(f"🔄 Retry attempt {retry_count}/{MAX_RETRIES} for offset {offset}. Waiting {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    time.sleep(REQUEST_DELAY)
                
                response = requests.get(API_BASE_URL, params={ 'limit': LIMIT, 'offset': offset })
                
                if response.status_code == 200:
                    request_successful = True
                    data = response.json()

                    total_count = data.get("count")
                    results += data.get("results", [])
                    offset += LIMIT
                    more_pages = data.get("next", False)

                else:
                    print(f"❌ HTTP Error: {response.status_code} - {response.text}")
                    retry_count += 1
                    
                    if retry_count > MAX_RETRIES:
                        print(f"❌ Max retries ({MAX_RETRIES}) reached for offset {offset}. Skipping to next page.")
                        failed_pages.append({
                            "limit": LIMIT,
                            "offset": offset,
                            "error": f"HTTP Error: {response.status_code}",
                            "timestamp": datetime.now().isoformat()
                        })
                        offset += LIMIT

            except Exception as e:
                print(f"❌ Error fetching data: {e}")
                retry_count += 1
                
                if retry_count > MAX_RETRIES:
                    print(f"❌ Max retries ({MAX_RETRIES}) reached for offset {offset}. Skipping to next page.")
                    failed_pages.append({
                        "limit": LIMIT,
                        "offset": offset,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    offset += LIMIT

    print(f"✅ Fetched {total_count} providers")

    return results

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'No providers found'
