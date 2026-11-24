"""STAGE -> месячные и годовые витрины смертей по странам."""

from pyspark.sql import functions as F



def make_mart(stage, period):
    # сначала складываем провинции, чтобы получить одну строку страны на день.
    daily = stage.groupBy("country", "date").agg(
        F.sum("cumulative_deaths").alias("cumulative_deaths"),
    ).withColumn("period_start", F.date_trunc(period, "date").cast("date"))
    # берем значение на последнюю доступную дату, а не максимум числа смертей
    mart = daily.groupBy("country", "period_start").agg(
        F.count("*").alias("observed_days"), F.min("date").alias("first_observed_date"),
        F.max("date").alias("last_observed_date"),
        F.max(F.struct("date", "cumulative_deaths")).alias("last_snapshot"), # структуры сравниваются по полям слева направо, поэтому сначала выберется самая поздняя дата, а с ней и ее итог
    )
    #для месяца конец берем через last_day, для года — первое января следующего года минус день
    end = F.last_day("period_start") if period == "month" else F.add_months("period_start", 12) - F.expr("INTERVAL 1 DAY")
    mart = mart.withColumn("period_end", end.cast("date"))
    # сравниваем фактическое число дат с календарным, високосный февраль тоже учтется.
    mart = mart.withColumn("expected_days", F.datediff("period_end", "period_start") + 1)
    mart = mart.withColumn("is_complete", (F.col("observed_days") == F.col("expected_days")))
    # отмечаем неполное покрытие календаря, итог при этом сохраняем
    if period == "month":
        mart = mart.withColumn("is_incomplete_month", F.col("observed_days") < F.col("expected_days"))
    # годовой флаг считаем по всем датам страны за год. Он нужен и в месячной витрине.
    coverage = stage.select("country", "date").distinct().groupBy(
        "country", F.year("date").alias("year")
    ).agg(F.count("*").alias("year_days")) #после distinct одна строка это одна дата страны, count считает сколько дат есть за год
    coverage = coverage.withColumn("is_incomplete_year", F.col("year_days") <
        F.dayofyear(F.make_date(F.col("year"), F.lit(12), F.lit(31))))
    # присоединяем признак года к каждому периоду, после соединения технический year убираем.
    mart = mart.withColumn("year", F.year("period_start")).join(
        coverage.select("country", "year", "is_incomplete_year"), ["country", "year"]
    ).drop("year")
    # из последнего снимка достаем только накопительный итог. Последняя дата уже есть отдельным полем
    return (mart
            .withColumn("cumulative_deaths_end", F.col("last_snapshot.cumulative_deaths"))
            .drop("last_snapshot")) # временная структура уже не нужна, само число вынесли в cumulative_deaths_end


def build_mart(spark, store):
    #обе витрины используют один STAGING, кешируем его на время этих двух расчетов
    stage = store.read(spark, "daily").cache()
    try:
        #считаем месяцы и годы отдельно, таблицы для них задаются в ямле
        for period, target in [("month", "monthly"), ("year", "yearly")]:
            store.write(make_mart(stage, period), target)
    finally:
        # после записи обеих витрин кеш больше не нужен, освобождаем его и при ошибке
        stage.unpersist()
