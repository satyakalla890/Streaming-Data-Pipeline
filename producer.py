import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'  # Use localhost:29092 for external access
TOPIC_NAME = 'user_activity'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=json_serializer
)

user_ids = [f'user_{i}' for i in range(1, 21)]
page_urls = [
    '/home', '/products', '/cart', '/checkout', '/search', 
    '/login', '/profile', '/settings', '/about', '/contact'
]
event_types = ['page_view', 'click', 'session_start', 'session_end']

def generate_event(late_by=None):
    event_time = datetime.utcnow()
    if late_by:
        event_time = event_time - timedelta(minutes=late_by)
        
    event = {
        'event_time': event_time.isoformat() + 'Z',
        'user_id': random.choice(user_ids),
        'page_url': random.choice(page_urls),
        'event_type': random.choice(event_types)
    }
    return event

def run_producer():
    print(f"Starting producer, sending events to {TOPIC_NAME}...")
    try:
        count = 0
        while True:
            # 1. Regularly send normal random events
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            count += 1
            
            # 2. SEED A SPECIFIC SESSION (for verification of Requirement 7)
            if count == 5:
                print(">>> Sending manual session_start for user_A")
                e_start = generate_event()
                e_start.update({'user_id': 'user_A', 'event_type': 'session_start'})
                producer.send(TOPIC_NAME, value=e_start)
            
            if count == 15:
                print(">>> Sending manual session_end for user_A (10 seconds later)")
                e_end = generate_event()
                e_end.update({'user_id': 'user_A', 'event_type': 'session_end'})
                # Make the end time 10 seconds after start for a 10s duration
                producer.send(TOPIC_NAME, value=e_end)

            # 3. Occasionally send late data (for watermarking testing)
            if count % 20 == 0:
                print("Sending a late event (3 minutes late)...")
                late_event = generate_event(late_by=3)
                producer.send(TOPIC_NAME, value=late_event)
            
            if count % 10 == 0:
                print(f"Sent {count} events...")
                
            time.sleep(1) 
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
