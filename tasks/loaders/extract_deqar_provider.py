from prefect import task

import os
import requests
import json
import re
from datetime import datetime
import time
import uuid
import pandas as pd
from io import StringIO


@task(name="extract_deqar_provider", retries=1, retry_delay_seconds=30)
def extract_deqar_provider(
    api_base_url: str = 'https://backend.testzone.eqar.eu/connectapi/v1/providers/',
    limit: int = 2000,
    offset: int = 0,
    max_retries: int = 3,
    retry_delay: int = 10,
    request_delay: int = 1,
):

    total_count = 0
    failed_pages = []

    print(f"🚀 Starting data collection from {api_base_url}")
    print(f"📊 Using pagination with LIMIT={limit}, starting at OFFSET={offset}")

    current_offset = offset
    more_pages = True

    results = []

    while more_pages:
        print(f"🔍 Fetching: {current_offset}-{current_offset+limit}")

        retry_count = 0
        request_successful = False

        while retry_count <= max_retries and not request_successful:
            try:
                if retry_count > 0:
                    print(f"🔄 Retry attempt {retry_count}/{max_retries} for offset {current_offset}. Waiting {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    time.sleep(request_delay)

                response = requests.get(api_base_url, params={ 'limit': limit, 'offset': current_offset })

                if response.status_code == 200:
                    request_successful = True
                    data = response.json()

                    total_count = data.get("count")
                    results += data.get("results", [])
                    current_offset += limit
                    more_pages = data.get("next", False)

                else:
                    print(f"❌ HTTP Error: {response.status_code} - {response.text}")
                    retry_count += 1

                    if retry_count > max_retries:
                        print(f"❌ Max retries ({max_retries}) reached for offset {current_offset}. Skipping to next page.")
                        failed_pages.append({
                            "limit": limit,
                            "offset": current_offset,
                            "error": f"HTTP Error: {response.status_code}",
                            "timestamp": datetime.now().isoformat()
                        })
                        current_offset += limit

            except Exception as e:
                print(f"❌ Error fetching data: {e}")
                retry_count += 1

                if retry_count > max_retries:
                    print(f"❌ Max retries ({max_retries}) reached for offset {current_offset}. Skipping to next page.")
                    failed_pages.append({
                        "limit": limit,
                        "offset": current_offset,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    current_offset += limit

    print(f"✅ Fetched {total_count} providers")

    return results
