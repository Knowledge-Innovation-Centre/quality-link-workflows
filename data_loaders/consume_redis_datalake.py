from mage_ai.streaming.sources.base_python import BasePythonSource
from typing import Callable

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source
from mage_ai.data_preparation.shared.secrets import get_secret_value
import redis
import time
import json

@streaming_source
class CustomSource(BasePythonSource):
    def init_client(self):

        redis_host = get_secret_value("DRAGONFLY_HOST")
        redis_password = get_secret_value("DRAGONFLY_PASSWORD")

        self.queue_name = "provider_data_queue"  
        self.r = redis.Redis(
            host=redis_host,
            port=6379,
            password=redis_password,
            db=1,
            decode_responses=False,
        )

        try:
            self.r.ping()
            print(f"✅ Connected to Redis at {redis_host}")
        except Exception as e:
            print(f"❌ Redis connection failed: {str(e)}")
            raise e

    def batch_read(self, handler: Callable):
        """
        Batch read messages from the Redis queue and process them with the handler.
        """
        print(f"🔄 Starting batch reader on queue '{self.queue_name}'")
        
        while True:
            try:

                result = self.r.brpop(self.queue_name, timeout=1)
                
                if result is None:
                    continue
                    
                queue_name, message = result
                
                try:
                    if isinstance(message, bytes):
                        message = message.decode('utf-8')
                    
                    record = json.loads(message)
                    print(f"📝 Processing item from {queue_name}")
                    
                    handler([record])
                    
                except json.JSONDecodeError as e:
                    print(f"⚠️ Failed to parse JSON: {str(e)}")
                    print(f"⚠️ Raw message: {message[:100]}...")
                    
                except Exception as e:
                    print(f"❌ Error processing message: {str(e)}")
                    
            except Exception as e:
                print(f"❌ Queue operation error: {str(e)}")
                time.sleep(1)
