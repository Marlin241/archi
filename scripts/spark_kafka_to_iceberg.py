import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
ICEBERG_BUCKET = os.getenv("MINIO_ICEBERG_BUCKET", "traffic-bucket")

spark = (
    SparkSession.builder
    .appName("KafkaToIceberg")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config("spark.sql.catalog.my_catalog.warehouse", f"s3a://{ICEBERG_BUCKET}/iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Optimisations
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)

SCHEMA = StructType([
    StructField("event_id",      IntegerType(), True),
    StructField("timestamp",     StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("section_id",    StringType(),  True),
    StructField("section_name",  StringType(),  True),
    StructField("direction",     StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("speed",         DoubleType(),  True),
    StructField("vehicle_count", IntegerType(), True),
])

raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "traffic_events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Column pruning + parsing + predicate pushdown en une passe
df = (
    raw_df.select(from_json(col("value").cast("string"), SCHEMA).alias("d"))
    .select("d.*")
    .filter(col("city").isNotNull() & col("speed").isNotNull())
)
df.cache()

# Crée la table Iceberg si elle n'existe pas encore
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.default.traffic_events (
        event_id      INT,
        timestamp     STRING,
        city          STRING,
        section_id    STRING,
        section_name  STRING,
        direction     STRING,
        latitude      DOUBLE,
        longitude     DOUBLE,
        speed         DOUBLE,
        vehicle_count INT
    )
    USING iceberg
    PARTITIONED BY (city)
""")

df.write.format("iceberg").mode("append").save("my_catalog.default.traffic_events")
print(f"[OK] {df.count()} événements écrits dans Iceberg.")

print("\n--- Vitesse moyenne par ville ---")
df.groupBy("city").agg(avg("speed").alias("vitesse_moy")).show()

print("\n--- Densité par section ---")
df.groupBy("section_id").agg(spark_sum("vehicle_count").alias("total_vehicules")).show()

print("\n--- Alertes vitesse > 90 km/h ---")
df.filter(col("speed") > 90).select("event_id", "city", "speed").show()

df.unpersist()
spark.stop()
print("\n[OK] Job Spark terminé avec succès.")
