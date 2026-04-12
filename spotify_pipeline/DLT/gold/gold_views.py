import dlt
import pyspark.sql.functions as F

catalog = spark.conf.get("catalog")


@dlt.materialized_view(
    name=f"{catalog}.gold.spotify_gold_ios_events",
    comment=" This is a gold view that calculates the popularity of songs based on the number of events and the average duration of the songs, this is the gold layer of the spotify pipeline",
    table_properties={"quality": "gold", "pipeline": "spotify_pipeline"},
)
def spotify_gold_ios_popularity():
    return (
        spark.table(f"{catalog}.silver.spotify_events")
        .filter(F.col("device_type") == "ios")
        .groupBy("Country", "device_type")
        .agg(
            F.count("user_id").alias("total_users"),
            F.count("track_id").alias("total_tracks"),
            F.sum(F.col("played_in_mins")).alias("total_played_in_mins"),
        )
    )


@dlt.materialized_view(
    name=f"{catalog}.gold.spotify_gold_android_events",
)
def spotify_gold_android_popularity():
    return (
        spark.table(f"{catalog}.silver.spotify_events")
        .filter(F.col("device_type") == "android")
        .groupBy("Country", "device_type")
        .agg(
            F.count("user_id").alias("total_users"),
            F.count("track_id").alias("total_tracks"),
            F.sum(F.col("played_in_mins")).alias("total_played_in_mins"),
        )
    )
