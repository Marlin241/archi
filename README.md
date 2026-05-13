# Smart City Traffic — Dakar

Pipeline Big Data de surveillance du trafic urbain en temps réel pour l'agglomération de Dakar (Sénégal).

## Aperçu

Les événements de trafic sont générés sur 24 axes routiers réels de Dakar, Pikine, Guédiawaye, Rufisque et Thiès, avec des coordonnées GPS précises et une simulation réaliste des heures de pointe. Les données transitent par Kafka, sont indexées dans Elasticsearch pour la visualisation temps réel, et stockées en format Iceberg dans MinIO pour l'analyse batch via Spark.

## Architecture

```
produce_traffic.py (DAG toutes les 2 min)
    └► Kafka (topic: traffic_events)
            ├► consume_traffic.py ─► Elasticsearch ─► Dashboard Streamlit
            └► Spark (DAG toutes les 15 min) ─► Iceberg / MinIO (couche bronze)
```

## Stack technique

| Composant | Technologie | Port |
|---|---|---|
| Message broker | Apache Kafka (KRaft) | 9092 (Docker), 9093 (Windows) |
| Stockage objet | MinIO (S3-compatible) | 9000 (API), 9001 (console) |
| Moteur de recherche | Elasticsearch 8.x | 9200 |
| Visualisation ES | Kibana | 5601 |
| Orchestration | Apache Airflow 2.9 | 8080 |
| Traitement batch | Apache Spark 3.5 + Iceberg | — |
| Notebooks | Jupyter PySpark | 8888 |
| Dashboard | Streamlit + Plotly | 8501 |
| Base de données | PostgreSQL (backend Airflow) | — |

## Démarrage rapide

### Prérequis

- Docker Desktop
- Python 3.10+ (pour les scripts lancés depuis Windows)

### Lancer toute l'infrastructure

```bash
docker-compose up -d
```

Les buckets MinIO sont créés automatiquement au démarrage. Attendre ~30 secondes que tous les services soient prêts.

### Accès aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Dashboard Streamlit | http://localhost:8501 | — |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kibana | http://localhost:5601 | — |
| Jupyter | http://localhost:8888 | — |
| Elasticsearch | http://localhost:9200 | — |

## DAGs Airflow

| DAG | Schedule | Description |
|---|---|---|
| `smart_city_final_v3` | toutes les 2 min | Produit 1 000 événements Kafka → indexation Elasticsearch |
| `spark_kafka_to_iceberg` | toutes les 15 min | Lecture Kafka → écriture table Iceberg sur MinIO |
| `check_smart_city_pipeline` | quotidien | Vérification de connectivité |

## Données simulées

### Sections couvertes (24 axes)

- **Dakar** : VDN (Hann, Liberté 6, Ouakam), Corniche (Soumbédioune, Fann, Almadies), Centre-ville (Plateau, Médina, Colobane), Ouakam / Almadies, Autoroute à péage
- **Pikine** : Marché Tilène, Tally Bou Bess, Guinaw Rail Nord, Route de Thiaroye, Autoroute Mbao
- **Guédiawaye** : Sam-Notaire, Golf Sud
- **Rufisque** : Centre-ville, Route de Sangalkam, Autoroute Diamniadio
- **Thiès** : Centre-ville, Route de Tivaouane

### Modèle de trafic

La simulation reproduit les patterns réels :

| Période | Vitesse | Véhicules |
|---|---|---|
| Heure de pointe matin (7h–9h) | 8–30 km/h | 25–60 |
| Journée (9h–17h) | 35–75 km/h | 8–25 |
| Heure de pointe soir (17h–20h) | 5–28 km/h | 30–70 |
| Nuit (23h–7h) | 60–115 km/h | 1–4 |
| Weekend | trafic réduit | — |

Les sections à forte congestion structurelle (marchés, carrefours centraux) appliquent un facteur de réduction de vitesse supplémentaire.

### Schéma d'un événement Kafka

```json
{
  "event_id": 42,
  "timestamp": "2026-05-13T08:30:00",
  "city": "Dakar",
  "section_id": "VDN-HANN",
  "section_name": "VDN — Échangeur de Hann",
  "direction": "N",
  "latitude": 14.7272,
  "longitude": -17.4334,
  "speed": 18.5,
  "vehicle_count": 47
}
```

## Scripts

| Script | Description |
|---|---|
| `scripts/produce_traffic.py` | Producteur Kafka (1 000 événements / run) |
| `scripts/consume_traffic.py` | Consommateur Kafka → Elasticsearch |
| `scripts/spark_kafka_to_iceberg.py` | Job Spark : Kafka → table Iceberg sur MinIO |
| `scripts/init_minio.py` | Création automatique des buckets MinIO |

### Exécution manuelle depuis Windows

```bash
# Produire des événements
pip install kafka-python
python scripts/produce_traffic.py

# Consommer vers Elasticsearch
pip install kafka-python requests
python scripts/consume_traffic.py
```

> Les scripts Windows utilisent le port **9093** pour Kafka et `localhost` pour tous les services.
> Les scripts dans Docker utilisent le port **9092** et les noms de services (`kafka`, `elasticsearch`, `minio`).

## Notebook d'analyse Spark

Le notebook `notebooks/spark_traffic_analysis.ipynb` illustre :

1. Lecture batch depuis Kafka
2. Écriture dans une table Iceberg partitionnée par ville
3. Calcul de KPIs (vitesse moyenne, densité, alertes vitesse > 90 km/h)
4. Techniques d'optimisation Spark : AQE, cache/persist, repartition, broadcast join, coalesce, Z-ordering Iceberg

## Stockage MinIO

- **Bucket** : `traffic-bucket`
- **Chemin Iceberg** : `traffic-bucket/iceberg/` (table `default.traffic_events`, partitionnée par ville)