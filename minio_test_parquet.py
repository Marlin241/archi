# minio_test_parquet.py

from minio import Minio
from minio.error import S3Error
from io import BytesIO
import pandas as pd

# 1. Connexion à MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

bucket_name = "traffic-bucket"

# 2. Créer le bucket s'il n'existe pas
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' créé.")
else:
    print(f"Bucket '{bucket_name}' déjà existant.")

# 3. Créer un petit DataFrame (ex: données trafic simulées)
df = pd.DataFrame({
    "timestamp": pd.date_range("2026-04-22 10:00:00", periods=5, freq="min"),
    "section_id": [1, 2, 1, 3, 2],
    "vehicle_count": [10, 25, 15, 8, 30],
    "city": ["Dakar"] * 5,
})

# 4. Sauver le DataFrame en Parquet dans un buffer
parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)

# 5. Nom de l'objet dans le bucket
object_name = "bronze/traffic-test.parquet"

# 6. Upload du fichier Parquet dans MinIO
client.put_object(
    bucket_name,
    object_name,
    data=parquet_buffer,
    length=parquet_buffer.getbuffer().nbytes,
    content_type="application/octet-stream",
)
print(f"Parquet uploadé dans MinIO: {bucket_name}/{object_name}")

# 7. Lister les objets du bucket pour vérifier
objects = client.list_objects(bucket_name, recursive=True)
print("\nObjets dans le bucket :")
for obj in objects:
    print(f"  - {obj.object_name}")