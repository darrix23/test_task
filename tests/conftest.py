import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (SparkSession.builder.master("local[2]").appName("covid-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session # отдаем одну сессию тестам, после них выполнение вернется сюда и ниже остановим спарк
    session.stop()
