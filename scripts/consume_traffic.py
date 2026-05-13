from kafka import KafkaConsumer
import json
import time
import requests

# CORRECTION : Utilise le port 9093 pour communiquer depuis Windows vers Docker
bootstrap_servers = ['localhost:9093']
topic = 'traffic_events'
ES_URL = "http://localhost:9200/traffic_events/_doc"

# Attendre ES (démarre lentement)
print("Attente Elasticsearch...")
for i in range(60):  # 1 minute max
    try:
        resp = requests.get("http://localhost:9200", timeout=5)
        if resp.status_code == 200:
            print("ES prêt")
            break
    except:
        print(f"ES retry {i+1}/60...")
        time.sleep(1)
else:
    print("ES lent - continuation sans indexation")

# Consumer Kafka
try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='traffic_consumer_group'
    )
    print(f"Listening Kafka '{topic}'...")
except Exception as e:
    print(f"Erreur de connexion Kafka : {e}")
    exit(1)

total = 0

for msg in consumer:
    try:
        event = msg.value
        total += 1
        print(f"#{total} - {event['city']} | Vitesse: {event['speed']} | {event['timestamp'][:19]}")
        
        # Index ES
        resp = requests.post(
            ES_URL,
            headers={"Content-Type": "application/json"},
            json=event,
            timeout=5
        )
        if resp.status_code in [200, 201]:
            print(f" -> ES indexé #{event.get('event_id', 'N/A')}")
        else:
            print(f" -> Erreur ES: {resp.status_code}")
            
    except Exception as e:
        print(f" -> Erreur traitement: {e}")
    
    if total % 10 == 0:
        print(f"Total messages traités: {total}")