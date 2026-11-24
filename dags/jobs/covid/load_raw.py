"""CSV -> RAW: исходные заголовки дат и значения строками."""

import hashlib
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F



# точки в заголовках дат Spark может принять за обращение к вложенному полю. оборачиваем имя в обратные кавычки, чтобы получить именно колонку с таким названием.
def quoted(name):
    return F.col("`" + name.replace("`", "``") + "`")


# в обоих форматах первым идет месяц. Эту функцию используем при построении STAGING
def parse_date(name):
    return datetime.strptime(name, "%m/%d/%y" if "/" in name else "%m.%d.%Y").date()



def prepare_raw(spark, path, year, run_id, options=None):
    # читаем CSV строками, без угадывания типов. В RAW сохраняем то, что пришло в источнике
    frame = spark.read.options(**(options or dict(header=True, sep=";", inferSchema=False, mode="FAILFAST"))).csv(str(path))
    if frame.columns[:2] != ["Country/Region", "Province/State"]:
        raise ValueError(f"Unexpected geography columns in {path}")
    #RAW не разбирает даты: сохраняем исходные поля. Распознавание — в STAGING
    unknown = [frame.columns[-1]] if frame.columns[-1].startswith("_c") else []
    # это количество будущих наблюдений на одну локацию, пока без проверки самих заголовков.
    date_columns = len(frame.columns) - 2 - len(unknown)
    # сохраняем исходную строку целиком в JSON, чтобы потом разобрать исключения без повторного чтения CSV
    payload = F.to_json(F.struct(*[quoted(c) for c in frame.columns]))
    frame = frame.withColumn("raw_payload", payload).withColumnRenamed("Country/Region", "country_raw")
    frame = frame.withColumnRenamed("Province/State", "province_raw")
    # у безымянного хвоста CSV будет имя вроде _c347. Даем ему одно техническое имя для всех лет
    if unknown:
        frame = frame.withColumnRenamed(unknown[0], "unmapped_value")
    else:
        frame = frame.withColumn("unmapped_value", F.lit(None).cast("string"))
    #Добавляем происхождение строки: файл, год и запуск. Даты в заголовках остаются как были
    frame = (frame.withColumn("source_file", F.lit(Path(path).name))
             .withColumn("source_year", F.lit(year)).withColumn("run_id", F.lit(run_id)))
    return frame, date_columns


def load_raw(spark, store):
    paths = [(year, Path(store.path(year))) for year in store.years]
    # сначала проверяем наличие всех файлов, чтобы не начинать загрузку с неполным набором.
    for _, path in paths:
        if not path.is_file():
            raise FileNotFoundError(path)
    # запоминаем, что загрузили, чтобы потом сверить количество строк
    manifest = []
    for year, path in paths:
        frame, dates = prepare_raw(spark, path, year, store.run_id,
                                  dict(header=True, sep=store.config["separator"], inferSchema=False, mode="FAILFAST"))
        # sHA256 описывает содержимое файла, а число строк и дат понадобится для проверки STAGING
        manifest.append((year, path.name, hashlib.sha256(path.read_bytes()).hexdigest(),
                         frame.count(), dates, store.run_id))
        store.write(frame, "raw", year)
    #Манифест пишем отдельно от данных, одна строка на загруженный файл
    store.write(spark.createDataFrame(manifest, "source_year int, source_file string, sha256 string, "
                "row_count long, date_columns long, run_id string"), "manifest")
