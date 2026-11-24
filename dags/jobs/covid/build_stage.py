"""RAW -> STAGE. Фильтрация стран, даты в строки, числовые типы."""

from functools import reduce

from pyspark.sql import Window, functions as F

from jobs.covid.load_raw import parse_date, quoted


def unpivot(frame):
    # служебные колонки не пытаемся разобрать как даты. Все остальные должны быть датами.
    metadata = {"country", "province", "country_raw", "province_raw", "source_file",
                "source_year", "run_id", "unmapped_value", "raw_payload", "exclusion_reason"}
    # даты пока лежат в названиях столбцов, разворачиваем их в строки
    dates = [(name, parse_date(name)) for name in frame.columns if name not in metadata]
    if not dates:
        raise ValueError("No date columns in RAW")
    # для каждой строки собираем пары дата/значение, explode делает из них отдельные строки. observation.* затем раскрывает поля структуры в обычные колонки date и source_value
    return frame.select("country", "province", "country_raw", "province_raw", "source_file", "source_year",
                        "run_id", "unmapped_value", F.explode(F.array(*[
                            F.struct(F.lit(day).alias("date"), quoted(name).alias("source_value"))
                            for name, day in dates
                        ])).alias("observation")).select(
                            "country", "province", "country_raw", "province_raw", "source_file", "source_year",
                            "run_id", "unmapped_value", "observation.*")


def classify(frame, parameters):
    #Названия берем как в файле. Убираем только ранее согласованные строки
    country = F.col("country_raw")
    # отсутствие провинции допустимо. Для ключа используем пустую строку вместо налл.
    province = F.coalesce(F.col("province_raw"), F.lit("")) # coalesce берет первое не налл значение, если провинции нет — получаем пустую строку
    # причина пока пустая. Заполняем ее только для согласованных исключений из YAML
    reason = F.lit(None).cast("string") #lit делает колонку с одинаковым значением во всех строках, здесь это пока строковый налл
    for source, message in parameters.get("excluded_countries", {}).items():
        reason = F.when(country == source, F.lit(message)).otherwise(reason) # совпала страна — ставим причину, не совпала — оставляем то что уже было
    # отдельно смотрим исключения внутри страны, например судовые строки у Canada
    for source, regions in parameters.get("excluded_regions", {}).items():
        for region, message in regions.items():
            reason = F.when((country == source) & (province == region), F.lit(message)).otherwise(reason)
    return frame.withColumn("country", country).withColumn("province", province).withColumn("exclusion_reason", reason)


def make_stage(frame):
    # одна локация на одну дату должна встретиться ровно один раз, иначе сложим ее дважды
    duplicates = frame.groupBy("country", "province", "date").count().where("count > 1")
    if duplicates.limit(1).count():
        raise ValueError("Duplicate location/date")
    #Накопительное значение приводим к целому числу. Плохая строка станет NULL, ниже это отловим
    stage = frame.withColumn("cumulative_deaths", F.expr("try_cast(trim(source_value) as bigint)"))
    if stage.where("cumulative_deaths IS NULL OR cumulative_deaths < 0 OR country IS NULL OR country = ''").limit(1).count():
        raise ValueError("Invalid cumulative deaths or country in STAGING")
    # смотрим соседние даты, чтобы заметить пропуски в ряду.
    window = Window.partitionBy("country", "province").orderBy("date")
    stage = stage.withColumn("previous_date", F.lag("date").over(window)) # lag берет дату предыдущей строки внутри этой страны и провинции, порядок задает окно
    if stage.where("previous_date IS NOT NULL AND datediff(date, previous_date) <> 1").limit(1).count():
        raise ValueError("Missing dates in location series")
    # значения оставляем накопительными. Вспомогательная предыдущая дата больше не нужна
    return stage.drop("unmapped_value", "previous_date")


def build_stage(spark, store):
    #Годы пока читаем отдельно, затем объединим их в одну длинную таблицу
    parameters, frames, excluded = store.config, [], []
    for year in store.years:
        frame = classify(store.read(spark, "raw", year), parameters)
        # исключенные строки не теряем: сохраняем исходные поля, джсон и причину отдельно от STAGING.
        excluded.append(frame.where("exclusion_reason IS NOT NULL").select(
            "country_raw", "province_raw", "source_file", "source_year", "exclusion_reason", "raw_payload", "run_id"))
        accepted = frame.where("exclusion_reason IS NULL")
        # после исключения Korea безымянный хвост должен быть пустым. Иначе есть новая проблема источника
        if accepted.where("unmapped_value IS NOT NULL").limit(1).count():
            raise ValueError("Unexpected nonempty column without date in accepted rows")
        frames.append(unpivot(accepted))
    # склеиваем годы по именам колонок и только потом проверяем непрерывность дат между годами
    store.write(reduce(lambda a, b: a.unionByName(b), excluded), "excluded")
    store.write(make_stage(reduce(lambda a, b: a.unionByName(b), frames)), "daily") #reduce склеивает первый df со вторым, результат с третьим и так до конца списка
