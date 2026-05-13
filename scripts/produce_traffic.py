from kafka import KafkaProducer
import json
import random
from datetime import datetime
import time

# CORRECTION : Utilise le port 9093 pour communiquer depuis Windows vers Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all',
    request_timeout_ms=20000
)

# Test rapide
try:
    # Le topic sera créé automatiquement par Kafka si KAFKA_AUTO_CREATE_TOPICS_ENABLE est true
    metadata = producer.partitions_for('traffic_events')
    print("Kafka prêt - topic accessible")
except Exception as e:
    print(f"Erreur lors de la connexion : {e}")

cities = ['Dakar', 'Pikine', 'Rufisque', 'Thiès']
directions = ['N', 'S', 'E', 'W']

print("Envoi 1000 événements...")
for i in range(1000):
    event = {
        'event_id': i,
        'timestamp': datetime.now().isoformat(),
        'city': random.choice(cities),
        'section_id': f"SEC-{random.randint(100, 150)}",
        'direction': random.choice(directions),
        'speed': round(random.uniform(10, 120), 1),
        'vehicle_count': random.randint(1, 10)
    }
    producer.send('traffic_events', event)
    
    if i % 200 == 0:
        print(f" {i}/1000 envoyés")

producer.flush()
producer.close()
print("Production terminée !")