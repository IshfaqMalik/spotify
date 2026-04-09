"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import os, sys, pathlib
import pytest

sys.path.append(os.getcwd())


@pytest.fixture()
def spark():
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        print("LOCAL SPARK SESSION CREATED")
    except ImportError:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
        print("DATABRICKS CONNECT SPARK SESSION CREATED")
    except:
        raise ImportError(
            "Failed to create SparkSession.\n\nEnsure you have the correct dependencies installed and configured"
        )
    return spark
