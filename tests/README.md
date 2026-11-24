Тестируем только код рабочих DAG: сборку задач и зависимостей, а также PySpark-обработчики из dags/jobs. Проверок Docker, PostgreSQL-транзакций и ноутбуков здесь нет.

```bash
docker compose exec -T airflow-scheduler python -m pytest /usr/local/airflow/tests -q
```
