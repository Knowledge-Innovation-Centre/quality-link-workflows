if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests
from typing import Dict, List, Any
from mage_ai.data_preparation.shared.secrets import get_secret_value


@data_loader
def load_data(*args, **kwargs) -> Dict[str, Any]:

    config = {
        "base_url": kwargs.get("base_url", "https://apigateway-ota.osiris-link.nl/api/tue/acc/ords/ooapi/v5"),
        "api_key": get_secret_value("EIND_API_KEY"),
        "consumer": kwargs.get("consumer", "eduxchange"),
        "alliance": kwargs.get("alliance", "ewuu"),
        "timeout": kwargs.get("timeout", 60)
    }
    
    headers = {
        "api-key": config['api_key']
    }
    
    params = {
        "consumer": config['consumer'],
        "alliances.name": config['alliance']
    }
    
    url = f"{config['base_url']}/courses"
    print(f"📡 Fetching courses from: {url}")
    print(f"   Consumer: {config['consumer']}, Alliance: {config['alliance']}")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=config['timeout'])
        response.raise_for_status()  
        
        data = response.json()
        items = data.get('items', [])
        
        print(f"✅ Success! Found {len(items)} courses")
        
        for i, course in enumerate(items[:5], 1):
            course_name = course.get('name', 'N/A')
            if isinstance(course_name, list):
                course_name = course_name[0].get('value', 'N/A') if course_name else 'N/A'
            course_id = course.get('courseId', 'N/A')
            print(f"   {i}. {course_name} (ID: {course_id})")
        
        if len(items) > 5:
            print(f"   ... and {len(items) - 5} more")
        
        return {
            "items": items,
        }
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {str(e)}")
        return {
            'items': [],
            'metadata': {
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                'error': str(e),
                'count': 0,
                'consumer': config['consumer'],
                'alliance': config['alliance'],
                'url': url
            }
        }
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        raise


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
