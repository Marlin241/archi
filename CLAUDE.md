# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Smart City Traffic** — pipeline Big Data temps réel de surveillance du trafic urbain à Dakar (Sénégal). Les événements de trafic sont produits dans Kafka, consommés vers Elasticsearch, stockés en Parquet/Iceberg dans MinIO, orchestrés par Airflow, et visualisés via Streamlit.

## Commands

### Démarrer l'infrastructure complète
```bash
docker-compose up -d
```

### Démarrer uniquement certains services
```bash
docker-compose up -d kafka zookeeper elasticsearch kibana
docker-compose up -d minio
docker-compose up -d postgres airflow-init airflow-webserver airflow-scheduler
```

### Produire des événements de trafic (depuis Windows, hors Docker)
```bash
pip install kafka-python
python scripts/produce_traffic.py
```

### Consommer les événements vers Elasticsearch (depuis Windows)
```bash
pip install kafka-python requests
python scripts/consume_traffic.py
```

### Lancer le dashboard Streamlit (Elasticsearch)
```bash
pip install streamlit elasticsearch pandas plotly
streamlit run dashboards/smart_City_traffic.py
```

### Lancer le dashboard Iceberg/MinIO
```bash
pip install streamlit pyiceberg
streamlit run dashboards/app.py
```

### Tester l'écriture Parquet dans MinIO
```bash
pip install minio pandas pyarrow
python minio_test_parquet.py
```

### Vérifier les services
```bash
# Kafka (depuis le conteneur)
docker exec -it <kafka_container> /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Elasticsearch
curl http://localhost:9200/_cluster/health

# Airflow UI
# http://localhost:8080  (admin / admin)

# MinIO Console
# http://localhost:9001  (minioadmin / minioadmin)

# Kibana
# http://localhost:5601

# Jupyter
# http://localhost:8888
```

## Architecture

### Flux de données
```
produce_traffic.py
    → Kafka (topic: traffic_events)
        → consume_traffic.py → Elasticsearch (index: traffic_events)
        → Spark Streaming (optionnel) → MinIO (Parquet/Iceberg, couche bronze)
```

### Services Docker et ports

| Service | Port(s) | Usage |
|---|---|---|
| Zookeeper | 2181 | Coordination Kafka |
| Kafka | 9092 (interne Docker), **9093** (externe Windows) | Message broker |
| MinIO | 9000 (API S3), 9001 (console) | Data lake objet |
| Elasticsearch | 9200 | Indexation et recherche |
| Kibana | 5601 | Visualisation ES |
| Airflow webserver | 8080 | Orchestration (admin/admin) |
| PostgreSQL | interne | Backend Airflow |
| Jupyter PySpark | 8888 | Notebooks (sans token) |

### Point critique réseau
Les scripts Python exécutés **hors Docker** (depuis Windows) doivent utiliser le port **9093** pour Kafka et `localhost` pour tous les services. Les scripts exécutés **dans Docker** utilisent le port **9092** et les noms de services (`kafka`, `elasticsearch`, `minio`).

### Schéma d'un événement Kafka (`traffic_events`)
```json
{
  "event_id": 0,
  "timestamp": "2026-05-13T10:00:00",
  "city": "Dakar | Pikine | Rufisque | Thiès",
  "section_id": "SEC-100 à SEC-150",
  "direction": "N | S | E | W",
  "speed": 10.0,
  "vehicle_count": 5
}
```

### DAGs Airflow
- `smart_city_final_v3` — pipeline principal (horaire) : install → produce → consume vers ES
- `check_smart_city_pipeline` — vérification quotidienne de la connectivité Kafka + Spark

### Stockage MinIO
- Bucket : `traffic-bucket`
- Chemin bronze : `bronze/traffic-test.parquet`
- Le catalogue Iceberg (`app.py`) utilise l'URI `http://minio:9000` et la table `default.traffic_events`

### Jupyter PySpark
Le conteneur `jupyter-pyspark` inclut les packages Spark pour lire depuis Kafka (Spark Structured Streaming), écrire en Iceberg, et accéder à MinIO via le connecteur Hadoop-AWS. Les notebooks sont montés depuis `./notebooks`.

## Credentials par défaut

| Service | Utilisateur | Mot de passe |
|---|---|---|
| MinIO | minioadmin | minioadmin |
| Airflow | admin | admin |
| PostgreSQL | airflow | airflow |
