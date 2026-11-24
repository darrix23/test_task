# сборка дага
from datetime import datetime, timedelta
from pathlib import Path
import yaml


def load_pipeline(path):
    config = yaml.safe_load(Path(path).read_text()) # safe_load превращает ямл в словарь
    config["_path"] = str(Path(path).resolve())
    steps = config["steps"]
    if not steps or len(steps) != len(set(steps)) or not set(steps) <= {"raw", "stage", "mart", "validate"}:
        raise ValueError("steps must contain unique supported stages")
    return config


def connection_environment(config):
    # реальные реквизиты Airflow подставит перед запуском, tojson сохранит кавычки и переносы в пароле
    ref = f"conn[{config['postgres_conn_id']!r}]"
    fields = {"host": "host", "port": "port", "dbname": "schema", "user": "login", "password": "password"}
    expression = "{" + ", ".join(f"{key!r}: {ref}.{value}" for key, value in fields.items()) + "}"
    return {"ETL_POSTGRES_JSON": "{{ " + expression + " | tojson }}"}


def build_dag(config):
    #импорты нужны только для сборки, Spark-скрипт использует из этого файла только чтение конфига
    from airflow import DAG
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    script = Path(__file__).resolve().parents[1] / "jobs/run_job.py"
    jar = "/opt/spark/jars/postgresql-42.7.0.jar"
    with DAG(config["dag_id"], start_date=datetime(2024, 1, 1), schedule=config.get("schedule"),
             catchup=False, max_active_runs=1, is_paused_upon_creation=True,
             default_args={"retries": 1, "retry_delay": timedelta(minutes=1)}) as dag:
        previous = None
        for step in config["steps"]:
            task = SparkSubmitOperator(
                task_id=step, application=str(script), conn_id="spark_default",
                application_args=["--config", config["_path"], "--task-id", step, "--run-id", "{{ run_id }}"],
                env_vars=connection_environment(config), jars=jar, driver_class_path=jar,
                driver_memory="1g", conf={"spark.sql.shuffle.partitions": "4"})
            if previous is not None:
                previous >> task #соединяем стадии в том порядке, в котором они записаны в ямле
            previous = task
    return dag
