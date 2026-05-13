import os
import time

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError as ESConnectionError

st.set_page_config(page_title="Smart City Traffic Dakar", layout="wide")

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")


@st.cache_resource
def get_es_client():
    return Elasticsearch(
        hosts=[ES_HOST],
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=5,
        verify_certs=False,
        ssl_show_warn=False,
        headers={
            "Accept": "application/vnd.elasticsearch+json; compatible-with=9",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=9",
        },
    )


def wait_for_es(url, retries=30, delay=2):
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(delay)
    return False


if not wait_for_es(ES_HOST):
    st.error(f"Impossible de joindre Elasticsearch sur {ES_HOST}.")
    st.stop()

es = get_es_client()

try:
    es.info()
except Exception as e:
    st.error(f"Connexion Elasticsearch impossible: {e}")
    st.stop()

st.title("Smart City Traffic Dakar")
st.subheader("Dashboard en temps réel des flux de trafic avec carte")

time_options = {
    "Dernière heure": "now-1h",
    "Dernières 2 heures": "now-2h",
    "Dernières 6 heures": "now-6h",
    "Dernières 24 heures": "now-24h",
}

selected_time_key = st.selectbox("Plage de temps", list(time_options.keys()))
time_range = time_options[selected_time_key]

with st.spinner("Récupération des données depuis ElasticSearch..."):
    try:
        res = es.search(
            index="traffic_events",
            body={
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": time_range
                        }
                    }
                },
                "size": 10000,
            },
        )
    except ESConnectionError as e:
        st.error(f"Erreur de connexion Elasticsearch: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Erreur lors de la requête Elasticsearch: {e}")
        st.stop()

hits = res["hits"]["hits"]

if not hits:
    st.warning("Aucune donnée de trafic trouvée dans cette plage de temps.")
    st.stop()

df = pd.DataFrame([hit["_source"] for hit in hits])
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

st.write("Données brutes (premiers 10 événements)")
st.dataframe(df.head(10))

st.markdown("### Filtres")

cities = sorted(df["city"].dropna().unique())
sections = sorted(df["section_id"].dropna().unique())
directions = sorted(df["direction"].dropna().unique())

selected_city = st.selectbox("Ville", ["Toutes"] + cities, index=0)
selected_sections = st.multiselect("Section(s)", sections, default=sections)
selected_directions = st.multiselect("Direction(s)", directions, default=directions)

df_filtered = df.copy()

if selected_city != "Toutes":
    df_filtered = df_filtered[df_filtered["city"] == selected_city]

if selected_sections:
    df_filtered = df_filtered[df_filtered["section_id"].isin(selected_sections)]

if selected_directions:
    df_filtered = df_filtered[df_filtered["direction"].isin(selected_directions)]

st.markdown(f"### Données après filtrage : {len(df_filtered)} événements")
st.dataframe(df_filtered.head(10))

st.markdown("### Carte de Dakar — sections de trafic")

fake_coords = {
    "SEC-100": (14.7159, -17.4757),
    "SEC-101": (14.7310, -17.4969),
    "SEC-102": (14.6950, -17.4480),
    "SEC-104": (14.7340, -17.4590),
    "SEC-110": (14.7065, -17.4520),
    "SEC-115": (14.7120, -17.4800),
    "SEC-120": (14.6980, -17.4700),
    "SEC-125": (14.7200, -17.4600),
    "SEC-130": (14.6950, -17.4300),
    "SEC-135": (14.7050, -17.4350),
    "SEC-140": (14.7150, -17.4550),
    "SEC-145": (14.7000, -17.4200),
    "SEC-149": (14.6900, -17.4100),
}

sections_group = df_filtered.groupby("section_id").agg(
    avg_speed=("speed", "mean"),
    total_vehicles=("vehicle_count", "sum"),
).reset_index()

sections_group["latitude"] = sections_group["section_id"].map(lambda sec: fake_coords.get(sec, (0, 0))[0])
sections_group["longitude"] = sections_group["section_id"].map(lambda sec: fake_coords.get(sec, (0, 0))[1])

sections_map = sections_group[(sections_group["latitude"] != 0) & (sections_group["longitude"] != 0)].copy()

if sections_map.empty:
    st.warning("Aucune section trouvée avec coordonnées pour la carte.")
else:
    fig_map = px.scatter_map(
        sections_map,
        lat="latitude",
        lon="longitude",
        size="total_vehicles",
        color="avg_speed",
        hover_name="section_id",
        hover_data=["avg_speed", "total_vehicles"],
        color_continuous_scale="Portland",
        size_max=30,
        map_style="carto-darkmatter",
        zoom=10,
        title="Volume de trafic et vitesse moyenne par section (Dakar)",
    )
    fig_map.update_layout(margin={"r": 0, "t": 30, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

st.markdown("### Vitesse moyenne par ville")
agg_city = df_filtered.groupby("city").agg(
    avg_speed=("speed", "mean"),
    total_vehicles=("vehicle_count", "sum"),
).reset_index()

if not agg_city.empty:
    fig1 = px.bar(
        agg_city,
        x="city",
        y="avg_speed",
        color="city",
        title="Vitesse moyenne par ville",
        text="avg_speed",
    )
    fig1.update_traces(texttemplate="%{text:.1f}", textposition="outside")
    st.plotly_chart(fig1, use_container_width=True)

st.markdown("### Nombre total de véhicules par ville")
if not agg_city.empty:
    fig2 = px.bar(
        agg_city,
        x="city",
        y="total_vehicles",
        color="city",
        title="Total véhicules par ville",
        text="total_vehicles",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("### Volume de véhicules par heure")
if not df_filtered.empty:
    df_hour = df_filtered.set_index("timestamp").resample("1h").agg({"vehicle_count": "sum"}).reset_index()
    if not df_hour.empty:
        fig3 = px.line(
            df_hour,
            x="timestamp",
            y="vehicle_count",
            title="Volume de véhicules par heure",
            markers=True,
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("Aucune donnée horaire après filtrage.")

st.markdown("### Top 10 sections de trafic")
if not sections_group.empty:
    sections_group["total_vehicles"] = sections_group["total_vehicles"].fillna(0)
    top_10 = sections_group.nlargest(10, "total_vehicles")
    fig4 = px.bar(
        top_10,
        x="section_id",
        y="total_vehicles",
        color="section_id",
        title="Top 10 sections par volume de trafic",
    )
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.warning("Aucune section trouvée après filtrage.")

st.markdown("### Vitesse moyenne par ville et direction")
agg_city_dir = df_filtered.groupby(["city", "direction"]).agg(
    avg_speed=("speed", "mean"),
    total_vehicles=("vehicle_count", "sum"),
).reset_index()

if not agg_city_dir.empty:
    fig5 = px.bar(
        agg_city_dir,
        x="city",
        y="avg_speed",
        color="direction",
        barmode="group",
        title="Vitesse moyenne par ville et direction",
    )
    st.plotly_chart(fig5, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour ce filtre combiné.")