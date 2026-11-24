-- создаем три схемы, IF NOT EXISTS позволяет не падать если схема уже есть
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS mart;

GRANT ALL ON SCHEMA raw TO airflow;
GRANT ALL ON SCHEMA stage TO airflow;
GRANT ALL ON SCHEMA mart TO airflow;