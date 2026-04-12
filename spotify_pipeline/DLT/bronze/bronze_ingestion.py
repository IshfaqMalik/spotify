import pyspark.sql.functions as F
import dlt

catalog = spark.conf.get("catalog")


base_path = f"/Volumes/{catalog}/landing/raw_data/operational_data/"


@dlt.table(
    name=f"{catalog}.bronze.spotify_bronze_events",
    comment=" This is raw data ingested from spotify events, this is the bronze layer of the spotify pipeline",
    table_properties={"quality": "bronze", "pipeline": "spotify_pipeline"},
)
def spotify_bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{base_path}/spotify_events/")
        .withColumn("ingest_timestamp", F.current_timestamp())
    )


@dlt.table(
    name=f"{catalog}.bronze.spotify_bronze_songs",
    comment=" This is raw data ingested from spotify songs, this is the bronze layer of the spotify pipeline",
)
def spotify_bronze_songs():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{base_path}/spotify_dims/tracks/")
        .withColumn("ingest_timestamp", F.current_timestamp())
    )


@dlt.table(
    name=f"{catalog}.bronze.spotify_bronze_users",
    comment=" This is raw data ingested from spotify users, this is the bronze layer of the spotify pipeline",
)
def spotify_bronze_users():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{base_path}/spotify_dims/users/")
        .withColumn("ingest_timestamp", F.current_timestamp())
    )
