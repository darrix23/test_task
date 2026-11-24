import json
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag
from etl.dag_factory import load_pipeline
from etl.dag_factory import build_dag, connection_environment

ROOT = Path(__file__).resolve().parents[1]


def test_factory_builds_graph():
    config = load_pipeline(ROOT / "dags/pipelines/covid_pipeline.yaml")
    dag = build_dag(config)
    assert set(dag.task_ids) == {"raw", "stage", "mart", "validate"}
    assert dag.get_task("stage").upstream_task_ids == {"raw"}
    assert dag.get_task("mart").upstream_task_ids == {"stage"}
    assert dag.get_task("validate").upstream_task_ids == {"mart"}
    assert dag.get_task("raw")._application.endswith("jobs/run_job.py")


def test_connection_json_preserves_special_characters():
    config = load_pipeline(ROOT / "dags/pipelines/covid_pipeline.yaml")
    value = connection_environment(config)["ETL_POSTGRES_JSON"]
    connection = SimpleNamespace(host="localhost", port=5432, schema="database", login="user",
                                 password='quotes"slash\\newline\n')
    rendered = build_dag(config).get_template_env().from_string(value).render(conn={"covid_postgres": connection})
    assert json.loads(rendered)["password"] == connection.password


def test_dags_load_without_errors():
    bag = DagBag(str(ROOT / "dags"), include_examples=False)
    assert not bag.import_errors
    assert set(bag.dags) == {"covid_pipeline", "example_dag", "example_postgres_read", "example_read_csv"}
    assert bag.dags["covid_pipeline"].fileloc.endswith("generated_dags.py")



def test_factory_uses_configured_step_order():
    config = load_pipeline(ROOT / "dags/pipelines/covid_pipeline.yaml")
    config["steps"] = ["raw", "stage"]
    dag = build_dag(config)
    assert set(dag.task_ids) == {"raw", "stage"}
    assert dag.get_task("stage").upstream_task_ids == {"raw"}


def test_flat_config_has_routes():
    config = load_pipeline(ROOT / "dags/pipelines/covid_pipeline.yaml")
    assert config["input_path"].format(year=2021).endswith("raw_2021.csv")
    assert config["tables"]["daily"] == "stage.covid_daily"
    assert "datasets" not in config and "tasks" not in config
