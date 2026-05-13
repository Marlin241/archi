import streamlit as st
from pyiceberg.catalog import load_catalog

# Connexion à ton Data Lakehouse MinIO
catalog = load_catalog("default", **{
    "uri": "http://minio:9000",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin"
})

st.title("Smart City Traffic Dashboard")

# Chargement de la table Iceberg
table = catalog.load_table("default.traffic_events")
df = table.scan().to_pandas()

st.write("Dernières données enregistrées :", df.tail(10))
st.line_chart(df[['timestamp', 'traffic_speed']])