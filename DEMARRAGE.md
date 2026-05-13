# Guide de démarrage — Smart City Dakar

## Prérequis

- **Docker Desktop** en cours d'exécution
- **Python 3.10+** installé (pour les scripts Windows)
- **8 Go RAM** alloués à Docker (Paramètres → Resources)

---

## 1. Vérifier le fichier `.env`

Le fichier `.env` à la racine du projet contient **toutes les variables d'environnement** lues par `docker-compose`. Il est déjà présent et pré-configuré — ne le modifier que si vous changez un mot de passe ou un nom de bucket.

```env
# PostgreSQL (backend Airflow)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow
AIRFLOW_FERNET_KEY=46BKJoQYlhXg2rPu5WKu95gHdopactgBM9p4HAGScGQ=
AIRFLOW_SECRET_KEY=949f6974e64906f0e7d581c853f68349479b1836a94f6974e64906f0
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKETS=traffic-bucket
MINIO_ICEBERG_BUCKET=traffic-bucket

# Réseau interne Docker (ne pas modifier)
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ELASTICSEARCH_URL=http://elasticsearch:9200

# Nom du conteneur Jupyter utilisé par le DAG Spark
JUPYTER_CONTAINER_NAME=architecturebigdata-jupyter-pyspark-1
```

> **Important** : le `.env` est volontairement exclu du dépôt Git (`.gitignore`).
> Si vous clonez le projet sur une nouvelle machine, copiez ce fichier manuellement
> avant de lancer `docker-compose up -d`.

---

## 2. Démarrer l'infrastructure

```bash
docker-compose up -d
```

Attendre **60 secondes** puis vérifier que tous les conteneurs sont `Up` :

```bash
docker-compose ps
```

Résultat attendu — tous les services en état `Up` ou `healthy` :

| Conteneur | Port(s) exposé(s) |
|---|---|
| zookeeper | 2181 |
| kafka | 9092, **9093** |
| elasticsearch | 9200 |
| kibana | 5601 |
| minio | 9000, 9001 |
| postgres | — |
| airflow-webserver | **8080** |
| airflow-scheduler | — |
| airflow-init | — (init uniquement) |
| jupyter-pyspark | **8888** |
| streamlit | **8501** |

---

## 3. Vérifier les services clés

### Elasticsearch

```bash
curl http://localhost:9200/_cluster/health
```

Réponse attendue : `"status":"green"` ou `"status":"yellow"`

### Kafka (depuis le conteneur)

```bash
docker exec -it $(docker ps -qf name=kafka) /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### MinIO

Ouvrir [http://localhost:9001](http://localhost:9001) → `minioadmin` / `minioadmin`
Vérifier que le bucket **traffic-bucket** existe (créé automatiquement).

### Airflow

Ouvrir [http://localhost:8080](http://localhost:8080) → `admin` / `admin`

---

## 4. Activer les DAGs Airflow

Dans l'interface Airflow, activer les DAGs suivants (toggle à gauche du nom) :

| DAG | Schedule | Rôle |
|---|---|---|
| `smart_city_final_v3` | toutes les 2 min | Kafka → Elasticsearch |
| `spark_kafka_to_iceberg` | toutes les 15 min | Kafka → Iceberg / MinIO |
| `check_smart_city_pipeline` | quotidien | Vérification de connectivité |

Une fois activés, le DAG `smart_city_final_v3` déclenche automatiquement la production
et la consommation des événements de trafic.

---

## 5. Produire des données manuellement (optionnel)

Si vous souhaitez injecter des données sans attendre le DAG :

```bash
# Installation des dépendances (une seule fois)
pip install kafka-python requests

# Produire 1 000 événements dans Kafka
python scripts/produce_traffic.py

# Consommer et indexer dans Elasticsearch
python scripts/consume_traffic.py
```

> **Important** : ces scripts tournent depuis Windows et utilisent le port **9093**.
> Depuis l'intérieur des conteneurs Docker, le port est **9092**.

---

## 6. Accéder aux dashboards

| Interface | URL | Identifiants |
|---|---|---|
| Dashboard Streamlit (ES) | [http://localhost:8501](http://localhost:8501) | — |
| Dashboard Iceberg (MinIO) | `streamlit run dashboards/app.py` | — |
| Kibana | [http://localhost:5601](http://localhost:5601) | — |
| Airflow | [http://localhost:8080](http://localhost:8080) | admin / admin |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| Jupyter PySpark | [http://localhost:8888](http://localhost:8888) | sans token |
| Elasticsearch API | [http://localhost:9200](http://localhost:9200) | — |

---

## 7. Vérifier que des données arrivent dans Elasticsearch

```bash
curl "http://localhost:9200/traffic_events/_count"
```

Si le résultat `count` est supérieur à 0, le pipeline est fonctionnel.

Pour voir les 3 derniers événements :

```bash
curl "http://localhost:9200/traffic_events/_search?size=3&sort=timestamp:desc&pretty"
```

---

## 8. Arrêter l'infrastructure

```bash
# Arrêt propre (conserve les données)
docker-compose down

# Arrêt + suppression des volumes (repart de zéro)
docker-compose down -v
```

---

## Dépannage rapide

| Problème | Cause probable | Solution |
|---|---|---|
| Variables d'environnement non trouvées | Fichier `.env` absent | Recréer le `.env` depuis le modèle ci-dessus (section 1) |
| Conteneur `airflow-webserver` en `Exit` | `airflow-init` pas terminé | Attendre 60 s puis `docker-compose up -d` |
| Kafka inaccessible depuis Windows | Mauvais port | Utiliser le port **9093** (pas 9092) |
| `traffic-bucket` absent dans MinIO | Init raté | `docker-compose restart minio` |
| Elasticsearch `red` | Mémoire insuffisante | Allouer 8 Go à Docker |
| Streamlit — aucune donnée | Index ES vide | Lancer `produce_traffic.py` puis `consume_traffic.py` |

---

## Flux de données résumé

```
produce_traffic.py
    └─► Kafka (topic: traffic_events, port 9093 depuis Windows)
            ├─► consume_traffic.py ──► Elasticsearch ──► Streamlit / Kibana
            └─► Spark (DAG 15 min)  ──► Iceberg / MinIO  ──► dashboards/app.py
```

**Orchestration** : le DAG `smart_city_final_v3` dans Airflow exécute l'ensemble de la chaîne
(install → produce → consume) toutes les 2 minutes sans intervention manuelle.
