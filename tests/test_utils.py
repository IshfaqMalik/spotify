import pyspark.sql.functions as F
from src.spotify.utils import to_datestamp, to_date
import datetime
from pyspark.sql.types import *
from pyspark.sql import Row


def test_to_datestamp(spark):

    data = [
        Row(value=int(1717239600)),
        Row(value=int(1717331400)),  #
    ]

    df = spark.createDataFrame(data)
    result_df = to_datestamp(df, "value", "datestamp")
    result = result_df.select("datestamp").collect()

    assert result[0][0] == datetime.datetime(2024, 6, 1, 11, 0, 0)
    assert result[1][0] == datetime.datetime(2024, 6, 2, 13, 30, 0)


def test_to_date(spark):
    data = [("2026-04-03T13:14:01.800161",), ("2026-05-04T14:15:02.900262",)]
    df = spark.createDataFrame(data, ["value"])
    resultdf = to_date(df, "value", "date")
    result = resultdf.select("date").collect()
    assert result[0][0] == datetime.date(2026, 4, 3)
    assert result[1][0] == datetime.date(2026, 5, 4)
