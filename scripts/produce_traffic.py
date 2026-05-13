from kafka import KafkaProducer
import json
import os
import random
from datetime import datetime
import time

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093').split(',')
EVENT_COUNT = int(os.getenv('EVENT_COUNT', '1000'))

# Sections réelles de l'agglomération de Dakar avec coordonnées GPS
SECTIONS = [
    # ── Dakar — VDN (Voie de Dégagement Nord) ────────────────────────────────
    {"section_id": "VDN-HANN",      "section_name": "VDN — Échangeur de Hann",            "city": "Dakar",      "latitude": 14.7272, "longitude": -17.4334, "directions": ["N", "S"]},
    {"section_id": "VDN-LIBERTE",   "section_name": "VDN — Liberté 6",                    "city": "Dakar",      "latitude": 14.7320, "longitude": -17.4580, "directions": ["N", "S"]},
    {"section_id": "VDN-OUAKAM",    "section_name": "VDN — Échangeur Ouakam",             "city": "Dakar",      "latitude": 14.7380, "longitude": -17.4850, "directions": ["N", "S"]},
    # ── Dakar — Corniche ──────────────────────────────────────────────────────
    {"section_id": "COR-SOUMB",     "section_name": "Corniche — Soumbédioune",            "city": "Dakar",      "latitude": 14.6920, "longitude": -17.4600, "directions": ["N", "S"]},
    {"section_id": "COR-FANN",      "section_name": "Corniche — Fann",                    "city": "Dakar",      "latitude": 14.7050, "longitude": -17.4680, "directions": ["N", "S"]},
    {"section_id": "COR-ALMAD",     "section_name": "Corniche — Almadies",                "city": "Dakar",      "latitude": 14.7450, "longitude": -17.5100, "directions": ["N", "S"]},
    # ── Dakar — Centre-ville ──────────────────────────────────────────────────
    {"section_id": "CTR-PLATEAU",   "section_name": "Av. Léopold Sédar Senghor",          "city": "Dakar",      "latitude": 14.6937, "longitude": -17.4441, "directions": ["E", "W"]},
    {"section_id": "CTR-MEDINA",    "section_name": "Av. Cheikh Anta Diop — Médina",     "city": "Dakar",      "latitude": 14.6980, "longitude": -17.4380, "directions": ["E", "W"]},
    {"section_id": "CTR-COLOBANE",  "section_name": "Av. Blaise Diagne — Colobane",      "city": "Dakar",      "latitude": 14.7100, "longitude": -17.4420, "directions": ["E", "W"]},
    # ── Dakar — Ouakam / Almadies ─────────────────────────────────────────────
    {"section_id": "OUA-ROND",      "section_name": "Route des Almadies — Ouakam",        "city": "Dakar",      "latitude": 14.7320, "longitude": -17.5000, "directions": ["E", "W"]},
    {"section_id": "OUA-PHARE",     "section_name": "Route de Ngor — Phare des Mamelles", "city": "Dakar",      "latitude": 14.7490, "longitude": -17.5200, "directions": ["E", "W"]},
    # ── Autoroute à péage ─────────────────────────────────────────────────────
    {"section_id": "AUTO-MALICK",   "section_name": "Autoroute à péage — Entrée Dakar",   "city": "Dakar",      "latitude": 14.7290, "longitude": -17.4380, "directions": ["E", "W"]},
    {"section_id": "AUTO-MBAO",     "section_name": "Autoroute à péage — Échangeur Mbao", "city": "Pikine",     "latitude": 14.7440, "longitude": -17.3450, "directions": ["E", "W"]},
    {"section_id": "AUTO-DIAMNIAD", "section_name": "Autoroute à péage — Diamniadio",    "city": "Rufisque",   "latitude": 14.7690, "longitude": -17.2570, "directions": ["E", "W"]},
    # ── Pikine ───────────────────────────────────────────────────────────────
    {"section_id": "PIK-MARCHE",    "section_name": "Marché Tilène — Pikine",             "city": "Pikine",     "latitude": 14.7550, "longitude": -17.3900, "directions": ["N", "S", "E", "W"]},
    {"section_id": "PIK-TALLY",     "section_name": "Tally Bou Bess — Pikine",           "city": "Pikine",     "latitude": 14.7620, "longitude": -17.3800, "directions": ["N", "S"]},
    {"section_id": "PIK-GUINAW",    "section_name": "Guinaw Rail Nord",                    "city": "Pikine",     "latitude": 14.7480, "longitude": -17.4000, "directions": ["N", "S"]},
    {"section_id": "PIK-THIAROYE", "section_name": "Route de Thiaroye",                  "city": "Pikine",     "latitude": 14.7400, "longitude": -17.3700, "directions": ["E", "W"]},
    # ── Guédiawaye ───────────────────────────────────────────────────────────
    {"section_id": "GUE-SAM",       "section_name": "Sam-Notaire — Guédiawaye",          "city": "Guédiawaye", "latitude": 14.7780, "longitude": -17.4050, "directions": ["N", "S"]},
    {"section_id": "GUE-GOLF",      "section_name": "Golf Sud — Guédiawaye",             "city": "Guédiawaye", "latitude": 14.7680, "longitude": -17.3980, "directions": ["E", "W"]},
    # ── Rufisque ─────────────────────────────────────────────────────────────
    {"section_id": "RUF-CENTRE",    "section_name": "Centre-ville Rufisque",              "city": "Rufisque",   "latitude": 14.7160, "longitude": -17.2680, "directions": ["N", "S", "E", "W"]},
    {"section_id": "RUF-SANGALKAM","section_name": "Route de Sangalkam",                 "city": "Rufisque",   "latitude": 14.7300, "longitude": -17.2900, "directions": ["N", "S"]},
    # ── Thiès ────────────────────────────────────────────────────────────────
    {"section_id": "THIES-CENTRE",  "section_name": "Centre-ville Thiès",                "city": "Thiès",      "latitude": 14.7910, "longitude": -16.9234, "directions": ["N", "S", "E", "W"]},
    {"section_id": "THIES-TIVAOUA","section_name": "Route de Tivaouane",                "city": "Thiès",      "latitude": 14.8100, "longitude": -16.8900, "directions": ["N", "S"]},
]

# Sections avec congestion structurellement plus forte (marchés, carrefours centraux)
HIGH_CONGESTION = {
    "CTR-PLATEAU", "CTR-MEDINA", "CTR-COLOBANE",
    "VDN-HANN", "PIK-MARCHE", "RUF-CENTRE", "THIES-CENTRE",
}


def get_traffic_params(hour: int, is_weekend: bool, section_id: str) -> tuple:
    """Retourne (speed_range, vehicle_range) selon l'heure, le jour et la section."""
    cf = 0.65 if section_id in HIGH_CONGESTION else 1.0  # facteur congestion vitesse
    vf = 1.50 if section_id in HIGH_CONGESTION else 1.0  # facteur volume véhicules

    if is_weekend:
        if 8 <= hour < 14:
            sr, vr = (30, 70), (8, 25)
        elif 14 <= hour < 20:
            sr, vr = (25, 55), (10, 30)
        else:
            sr, vr = (55, 110), (1, 6)
    else:
        if 7 <= hour < 9:       # heure de pointe matin
            sr, vr = (8, 30),   (25, 60)
        elif 9 <= hour < 12:
            sr, vr = (35, 70),  (10, 25)
        elif 12 <= hour < 14:   # pause déjeuner
            sr, vr = (20, 50),  (15, 35)
        elif 14 <= hour < 17:
            sr, vr = (40, 75),  (8, 20)
        elif 17 <= hour < 20:   # heure de pointe soir
            sr, vr = (5, 28),   (30, 70)
        elif 20 <= hour < 23:
            sr, vr = (45, 85),  (5, 15)
        else:                    # nuit
            sr, vr = (60, 115), (1, 4)

    speed_min = max(5, int(sr[0] * cf))
    speed_max = max(speed_min + 5, int(sr[1] * cf))
    veh_min   = max(1, vr[0])
    veh_max   = max(veh_min + 1, int(vr[1] * vf))
    return (speed_min, speed_max), (veh_min, veh_max)


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all',
    request_timeout_ms=20000,
)

try:
    producer.partitions_for('traffic_events')
    print("Kafka prêt.")
except Exception as e:
    print(f"Erreur Kafka : {e}")

now = datetime.now()
hour       = now.hour
is_weekend = now.weekday() >= 5

print(f"Production de {EVENT_COUNT} événements (heure={hour}h, weekend={is_weekend})...")

for i in range(EVENT_COUNT):
    section = random.choice(SECTIONS)
    speed_range, vehicle_range = get_traffic_params(hour, is_weekend, section["section_id"])

    event = {
        "event_id":      i,
        "timestamp":     datetime.now().isoformat(),
        "city":          section["city"],
        "section_id":    section["section_id"],
        "section_name":  section["section_name"],
        "direction":     random.choice(section["directions"]),
        "latitude":      section["latitude"],
        "longitude":     section["longitude"],
        "speed":         round(random.uniform(*speed_range), 1),
        "vehicle_count": random.randint(*vehicle_range),
    }
    producer.send('traffic_events', event)

    if i % 200 == 0:
        print(f"  {i}/{EVENT_COUNT} envoyés")

producer.flush()
producer.close()
print(f"Production terminée — {EVENT_COUNT} événements envoyés.")
