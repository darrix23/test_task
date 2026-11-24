"""регистрация пайплайнов из YAML"""

from pathlib import Path
from etl.dag_factory import load_pipeline, build_dag

# конфиги лежат рядом, каждый ямл дает отдельный DAG
for path in sorted((Path(__file__).parent / "pipelines").glob("*.yaml")):
    config = load_pipeline(path)
    name = config["dag_id"]
    if name in globals():
        raise ValueError(f"Duplicate DAG id: {name}")
    globals()[name] = build_dag(config) # Airflow заберет объект DAG из этого модуля
