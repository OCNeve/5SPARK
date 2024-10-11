from mastodon import Mastodon
from kafka import KafkaProducer
import json
import time

time.sleep(5)

mastodon = Mastodon(
    access_token=' Sl3zM4M4XPuG5GelIfK1JIZw7FCgeTDOBHdY3uaNotI',  # Remplacez par votre jeton d'accès
    api_base_url='https://mastodon.social'  # L'URL de votre instance Mastodon
)

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Kafka server running in Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

def stream_toots():
    while True:
        toots = mastodon.timeline_hashtag('DataScience')  # Example hashtag
        for toot in toots:
            toot_data = {
                'user_id': toot['account']['id'],
                'content': toot['content'],
                'timestamp': toot['created_at'].isoformat(),
                'favourites': toot['favourites_count'],
                'reblogs': toot['reblogs_count'],
                'hashtags': [tag['name'] for tag in toot['tags']]
            }
            # Serialize the dictionary and send it to Kafka
            producer.send('mastodonStream', value=toot_data)
            print(f"Sent toot to Kafka: {toot_data}")
        
        time.sleep(10)

stream_toots()
