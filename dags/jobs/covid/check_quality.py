"""DQ-отчет в PostgreSQL и логах задачи validate"""

from pyspark.sql import functions as F



def check_quality(spark, store):
    run_id = store.run_id
    stage = store.read(spark, "daily").cache()
    # сюда складываем результаты проверок. Потом запишем их одной таблицей для этого запуска.
    reports = []

    # по умолчанию ждем ноль ошибок. Для количества строк явно передаем ожидаемое значение
    def check(name, observed, expected=0, severity="error"):
        reports.append((name, severity, int(observed), int(expected), observed == expected, run_id)) # складываем одну проверку кортежем, passed получается обычным сравнением факта с ожиданием

    try:
        manifest = store.read(spark, "manifest")
        excluded = store.read(spark, "excluded").groupBy("source_year").count()
        #из числа исходных строк вычитаем исключения, умножаем на число столбцов дат Получаем ожидаемое количество строк после разворота RAW в STAGING
        expected = manifest.join(excluded, "source_year", "left").selectExpr(
            "sum((row_count - coalesce(count, 0)) * date_columns)"
        ).first()[0]
        check("stage_row_count", stage.count(), expected)
        check("invalid_stage_values", stage.where("cumulative_deaths IS NULL OR cumulative_deaths < 0").count())
        check("duplicate_location_dates", stage.groupBy("country", "province", "date").count().where("count > 1").count())
        # в витрину должны доехать те же названия, которые пришли из CSV.
        check("changed_country_names", stage.where(~F.col("country").eqNullSafe(F.col("country_raw"))).count())
        # для сверки витрины отдельно считаем накопительный итог страны на каждую дату
        daily = stage.groupBy("country", "date").agg(F.sum("cumulative_deaths").alias("total"))
        key = "monthly"
        table = store.table(key)
        mart = store.read(spark, key)
        check(table + ":duplicate_keys", mart.groupBy("country", "period_start").count().where("count > 1").count())
        expected = daily.withColumn("period_start", F.trunc("date", "month")).groupBy(
            "country", "period_start"
        ).agg(F.max(F.struct("date", "total")).alias("snapshot"))
        check(table + ":row_count", mart.count(), expected.count())
        #сверяем значение страны на последнюю дату месяца, разниц и сумм по датам здесь нет
        comparison = mart.join(expected, ["country", "period_start"], "full")
        check(table + ":snapshot_matches_stage", comparison.where(
            ~F.col("deaths").eqNullSafe(F.col("snapshot.total")) |
            ~F.col("last_observed_date").eqNullSafe(F.col("snapshot.date"))).count())
        check(table + ":partial_periods", mart.where("NOT is_complete").count(), severity="warning")
        check("excluded_source_rows", store.read(spark, "excluded").count(), severity="warning")
        # отчет сразу пишем в stage.quality_report, чтобы видеть причины даже если проверка упадет
        report = spark.createDataFrame(reports, "check_name string, severity string, observed long, expected long, passed boolean, run_id string")
        store.write(report, "quality")
        report.select("check_name", "severity", "observed", "expected", "passed").show(50, truncate=False)
        #если обязательная проверка не прошла, задача и весь DAG должны завершиться с ошибкой
        if any(not passed and severity == "error" for _, severity, _, _, passed, _ in reports):
            raise ValueError("Data quality checks failed; see stage.quality_report")
    finally:
        stage.unpersist()
