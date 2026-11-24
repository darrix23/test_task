from datetime import date, timedelta
from pathlib import Path

import pytest

from jobs.covid.build_mart import make_mart
from jobs.covid.build_stage import make_stage, unpivot
from jobs.covid.load_raw import parse_date, prepare_raw
from jobs.covid.build_stage import classify

ROOT = Path(__file__).resolve().parents[1]
from etl.dag_factory import load_pipeline
PARAMETERS = load_pipeline(ROOT / "dags/pipelines/covid_pipeline.yaml")


def source(spark, points, province=""):
    return spark.createDataFrame([
        ("France", province, "France", province, "sample.csv", day.year, "test", None, day, value)
        for day, value in points
    ], "country string, province string, country_raw string, province_raw string, source_file string, "
       "source_year int, run_id string, unmapped_value string, date date, source_value string")


@pytest.mark.parametrize("text,expected", [("1/13/21", date(2021, 1, 13)),
    ("01.02.2021", date(2021, 1, 2)), ("2/29/20", date(2020, 2, 29))])
def test_dates(text, expected):
    assert parse_date(text) == expected


def test_invalid_date():
    with pytest.raises(ValueError):
        parse_date("2/29/21")


def test_source_country_names_and_exclusions(spark):
    # проверяем на нескольких строках: имена не меняем, Korea и судно убираем.
    frame = spark.createDataFrame([
        ("US", ""), ("Burma", None), ("West Bank and Gaza", ""),
        ("Korea", " South"), ("MS Zaandam", ""),
    ], "country_raw string, province_raw string")
    rows = classify(frame, PARAMETERS).collect()
    assert [r.country for r in rows[:3]] == ["US", "Burma", "West Bank and Gaza"]
    assert all(r.exclusion_reason is None for r in rows[:3])
    assert all(r.exclusion_reason for r in rows[3:])


def test_stage_preserves_cumulative_values(spark):
    frame = source(spark, [(date(2020, 12, 31), "10"), (date(2021, 1, 1), "15"), (date(2021, 1, 2), "13")])
    rows = make_stage(frame).orderBy("date").collect() # collect забирает маленький тестовый df в питон, чтобы можно было сравнить значения обычным assert
    assert [r.cumulative_deaths for r in rows] == [10, 15, 13]
    assert "daily_deaths" not in rows[0].asDict()


@pytest.mark.parametrize("points,message", [
    ([(date(2021, 1, 1), "oops")], "Invalid cumulative"),
    ([(date(2021, 1, 1), "1"), (date(2021, 1, 1), "2")], "Duplicate"),
    ([(date(2021, 1, 1), "1"), (date(2021, 1, 3), "2")], "Missing dates"),
])
def test_invalid_stage_fails(spark, points, message):
    with pytest.raises(ValueError, match=message):
        make_stage(source(spark, points))


def test_complete_month_and_partial_year(spark):
    # на последнюю дату у провинций 128 и 262, вместе 390. Даты не суммируем.
    days = [date(2020, 12, 31) + timedelta(days=i) for i in range(32)]
    a = source(spark, [(d, str(100 + i - (3 if i >= 15 else 0))) for i, d in enumerate(days)], "A")
    b = source(spark, [(d, str(200 + 2 * i)) for i, d in enumerate(days)], "B")
    stage = make_stage(a.unionByName(b))
    january = make_mart(stage, "month").where("period_start = date '2021-01-01'").first()
    assert january.cumulative_deaths_end == 390
    assert january.observed_days == 31 and january.is_complete
    assert not january.is_incomplete_month
    year = make_mart(stage, "year").where("period_start = date '2021-01-01'").first()
    assert year.cumulative_deaths_end == 390 and year.is_incomplete_year
    december = make_mart(stage, "month").where("period_start = date '2020-12-01'").first()
    assert december.is_incomplete_month and december.cumulative_deaths_end == 300


def test_last_value_is_not_maximum(spark):
    stage = make_stage(source(spark, [(date(2021, 1, 1), "20"), (date(2021, 1, 2), "18")]))
    row = make_mart(stage, "month").first()
    assert row.cumulative_deaths_end == 18
    assert not {"deaths", "unknown_days", "negative_corrections", "dq_has_corrections", "dq_has_unknown_changes"} & set(row.asDict())


def test_leap_year_period(spark):
    frame = source(spark, [(date(2020, 2, 28), "10"), (date(2020, 2, 29), "11")])
    row = make_mart(make_stage(frame), "month").first()
    assert row.expected_days == 29 and row.observed_days == 2 and row.cumulative_deaths_end == 11


def test_raw_preserves_unmapped_values(spark, tmp_path):
    path = tmp_path / "raw_2021.csv"
    path.write_text("Country/Region;Province/State;01.01.2021;\nFrance;;10;99\n")
    raw, _ = prepare_raw(spark, path, 2021, "test")
    assert raw.first().unmapped_value == "99"


def test_raw_keeps_date_headers_and_stage_unifies_them(spark, tmp_path):
    path = tmp_path / "raw_2021.csv"
    path.write_text("Country/Region;Province/State;01.01.2021;1/2/21;\nFrance;;10;12;\n")
    raw, _ = prepare_raw(spark, path, 2021, "test")
    assert "01.01.2021" in raw.columns and "1/2/21" in raw.columns
    assert not any(name.startswith("d_") for name in raw.columns)
    assert raw.schema["01.01.2021"].dataType.simpleString() == "string"
    stage = make_stage(unpivot(classify(raw, PARAMETERS)))
    assert stage.schema["date"].dataType.simpleString() == "date"
    rows = stage.orderBy("date").collect()
    assert [r.date for r in rows] == [date(2021, 1, 1), date(2021, 1, 2)]
    assert [r.cumulative_deaths for r in rows] == [10, 12]


def test_korea_excluded_and_raw_keeps_every_row(spark):
    raw, _ = prepare_raw(spark, ROOT / "data/raw_2020.csv", 2020, "test")
    assert raw.count() == 289
    classified = classify(raw, PARAMETERS)
    korea = classified.where("country_raw = 'Korea'").collect()
    assert len(korea) == 2
    assert all(row.exclusion_reason == "untrusted_shifted_source" for row in korea)
    assert classified.where("exclusion_reason IS NULL").count() == 278


@pytest.mark.parametrize("year,days", [(2020,366),(2021,365)])
def test_full_calendar_year_flag(spark, year, days):
    start = date(year,1,1) - timedelta(days=1)
    stage = make_stage(source(spark, [(start + timedelta(days=i), str(100+i)) for i in range(days+1)]))
    row = make_mart(stage, "year").where(f"period_start = date '{year}-01-01'").first()
    assert not row.is_incomplete_year
    assert row.cumulative_deaths_end == 100 + days


def test_unrecognized_date_is_rejected_in_stage_not_raw(spark, tmp_path):
    path = tmp_path / "raw_2021.csv"
    path.write_text("Country/Region;Province/State;not-a-date;\nFrance;;10;\n")
    raw, _ = prepare_raw(spark, path, 2021, "test")
    assert "not-a-date" in raw.columns
    with pytest.raises(ValueError):
        unpivot(classify(raw, PARAMETERS))
