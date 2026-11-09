"""Fixtures de pytest compartidas.

- En Databricks reutiliza la sesión Spark del notebook (Serverless no permite crear una local).
- En local / CI crea una SparkSession local con ANSI activo, igual que Serverless.
"""
import os
import sys

import pytest

sys.dont_write_bytecode = True  # evita escribir __pycache__ en el repo
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        yield SparkSession.builder.getOrCreate()
    else:
        yield (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.ansi.enabled", "true")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false").getOrCreate())