import pyspark.sql.functions as F
import dlt

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.silver.spotify_silver_events",
    comment=" This is cleaned and transformed data from spotify events, this is the silver layer of the spotify pipeline",
    table_properties={"quality": "silver", "pipeline": "spotify_pipeline"},
)
@dlt.expect_or_drop("valid event_id", "event_id IS NOT NULL")
def spotify_silver_events():
    return (
        spark.readStream.table(f"{catalog}.bronze.spotify_bronze_events")
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_time"))
    )


@dlt.table(
    name=f"{catalog}.silver.spotify_silver_songs",
    comment=" This is cleaned and transformed data from spotify songs, this is the silver layer of the spotify pipeline",
    table_properties={"quality": "silver", "pipeline": "spotify_pipeline"},
)
@dlt.expect_or_drop("valid track_id", "track_id IS NOT NULL")
def spotify_silver_songs():
    return (
        spark.read.table(f"{catalog}.bronze.spotify_bronze_songs")
        .withColumn("track_age", F.date_diff(F.current_date(), F.to_date("release_date", "yyyy-MM-dd")))
        .withColumn("duration_in_mins", (F.col("duration_ms") / 60000).cast("decimal(10,2)"))
    )


@dlt.table(
    name=f"{catalog}.silver.spotify_silver_users",
    comment=" This is cleaned and transformed data from spotify users, this is the silver layer of the spotify pipeline",
    table_properties={"quality": "silver", "pipeline": "spotify_pipeline"},
)
@dlt.expect_or_drop("valid user_id", "user_id IS NOT NULL")
def spotify_silver_users():
    return spark.read.table(f"{catalog}.bronze.spotify_bronze_users").withColumn(
        "member_time", F.date_diff(F.current_date(), F.to_date("signup_date", "yyyy-MM-dd"))
    )
