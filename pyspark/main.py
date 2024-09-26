from mastodon_manager import Mastodon_manager
from producer import Producer 
import time 

api = Mastodon_manager().mastodon
producer = Producer().producer

def stream_toots():
    while True:
        toots = api.timeline_hashtag('DataScience')  # Example hashtag
        for toot in toots:
            toot_data = {
                'user_id': toot['account']['id'],
                'content': toot['content'],
                'timestamp': toot['created_at'].isoformat(),
                'favourites': toot['favourites_count'],
                'reblogs': toot['reblogs_count'],
                'hashtags': [tag['name'] for tag in toot['tags']]
            }
            producer.send('mastodon_stream', toot_data)
            print(f"Sent toot to Kafka: {toot_data}")
        time.sleep(10)  # Control API rate limits

if __name__ == '__main__':
    stream_toots()