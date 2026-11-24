"""Единая точка входа SparkSubmit: YAML + task_id определяют действие и маршруты."""

import argparse
from pathlib import Path
import sys

# Spark запускает этот файл как скрипт. Добавляем dags в поиск модулей, чтобы увидеть etl и jobs.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.dag_factory import load_pipeline
from etl.io import DatasetStore


def run_job(config_path, task_id, run_id):
    # spark и обработчики импортируем при запуске задачи, а не при чтении DAG планировщиком
    from pyspark.sql import SparkSession
    from jobs.covid.load_raw import load_raw
    from jobs.covid.build_stage import build_stage
    from jobs.covid.build_mart import build_mart
    from jobs.covid.check_quality import check_quality

    # по названию стадии выбираем отдельно импортированный обработчик
    registry = {"raw": load_raw, "stage": build_stage, "mart": build_mart, "validate": check_quality}
    config = load_pipeline(config_path)
    if task_id not in config["steps"]:
        raise ValueError(f"Stage not enabled: {task_id}")
    spark = SparkSession.builder.appName(config["dag_id"] + ":" + task_id).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # передаем обработчику Spark и помощника для чтения/записи, у него уже есть конфиг
        registry[task_id](spark, DatasetStore(config, run_id))
    finally:
        # даже если обработчик упал, освобождаем ресурсы Spark.
        spark.stop()


#эти аргументы заполняет SparkSubmitOperator. Но можно передать их и при ручном запуске
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_job(args.config, args.task_id, args.run_id)
