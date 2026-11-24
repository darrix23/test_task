"""Общее чтение и запись, пути и таблицы берем из ямла"""

from contextlib import closing
import json
import os
import re

import psycopg2
from psycopg2 import sql


class DatasetStore:
    def __init__(self, config, run_id):
        self.config, self.run_id = config, run_id
        self.years = config["years"]

    def path(self, year):
        return self.config["input_path"].format(year=year)

    def table(self, key, year=None):
        return self.config["tables"][key].format(year=year)

    def options(self):
        # одно подключение ко всем слоям, реквизиты приходят из Airflow через енв
        pg = json.loads(os.environ["ETL_POSTGRES_JSON"])
        jdbc = dict(url=f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}",
                    user=pg["user"], password=pg["password"], driver="org.postgresql.Driver")
        return pg, jdbc

    def read(self, spark, key, year=None):
        _, options = self.options()
        return spark.read.format("jdbc").options(**options, dbtable=self.table(key, year)).load()

    def write(self, frame, key, year=None):
        table = self.table(key, year)
        if not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", table):
            raise ValueError(f"Invalid table: {table}")
        pg, options = self.options()
        # схему создаем при первом запуске, closing закроет соединение после служебного SQL
        with closing(psycopg2.connect(**pg)) as conn, conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(table.split(".")[0])))
        #пишем прямо в таблицу слоя, повторный запуск полностью заменит ее содержимое
        frame.coalesce(2).write.format("jdbc").options(**options, dbtable=table).mode("overwrite").save()
