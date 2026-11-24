"""Месячная витрина: значение смертей страны на последнюю доступную дату"""

from pyspark.sql import functions as F


def make_mart(stage):
    # сначала складываем провинции на одну дату, чтобы получить значение страны из исходных данных
    daily = stage.groupBy("country", "date").agg(F.sum("cumulative_deaths").alias("deaths"))
    monthly = daily.withColumn("period_start", F.trunc("date", "month")).groupBy("country", "period_start").agg(
        F.max(F.struct("date", "deaths")).alias("snapshot"), # дата стоит первой, поэтому берем самое позднее наблюдение, а не максимальное число смертей
        F.count("*").alias("observed_days"), F.min("date").alias("first_observed_date"),
    )
    monthly = (monthly.withColumn("deaths", F.col("snapshot.deaths"))
               .withColumn("last_observed_date", F.col("snapshot.date")).drop("snapshot")
               .withColumn("period_end", F.last_day("period_start"))
               .withColumn("expected_days", F.dayofmonth("period_end")))
    #если месяц представлен не целиком, отмечаем это, само значение из источника не достраиваем
    return (monthly.withColumn("is_incomplete_month", F.col("observed_days") < F.col("expected_days"))
            .withColumn("is_complete", ~F.col("is_incomplete_month")))


def build_mart(spark, store):
    store.write(make_mart(store.read(spark, "daily")), "monthly")
