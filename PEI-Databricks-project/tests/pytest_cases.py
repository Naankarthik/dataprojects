import pytest
from pyspark.sql.functions import round

@pytest.fixture(scope="module")
def spark_session(spark):
    return spark

# Test that DataFrame schema contains 'id' column
def test_schema(spark_session):
    df = spark_session.createDataFrame([(1,"John")],["id","name"])
    assert "id" in df.columns

# Test that DataFrame is not empty
def test_not_empty(spark_session):
    df = spark_session.createDataFrame([(1,)],["id"])
    assert df.count() > 0

# Test that empty DataFrame has zero rows
def test_empty_df(spark_session):
    df = spark_session.createDataFrame([], "id INT")
    assert df.count() == 0

@pytest.mark.parametrize("input_value,expected", [
    (10.456, 10.46),
    (5.123, 5.12),
    (0.999, 1.0)
])
# Test rounding positive values to 2 decimal places
def test_rounding(spark_session, input_value, expected):
    df = spark_session.createDataFrame([(1, input_value)], ["id", "profit"])
    result = df.withColumn("profit", round("profit", 2))
    assert pytest.approx(result.collect()[0]["profit"], 0.01) == expected

# Test detection of duplicate order_id values
def test_duplicate_detection(spark_session):
    df = spark_session.createDataFrame([(1,),(1,)],["order_id"])
    dup = df.groupBy("order_id").count().filter("count>1")
    assert dup.count() == 1

@pytest.fixture
def spark_fixture(spark):
    return spark

@pytest.mark.parametrize("data,filter_col,filter_val,expected_count", [
    ([(2024, 100), (2023, 50)], "year", 2024, 1)
])
# Test filtering DataFrame by partition column value
def test_partition_filtering(spark_fixture, data, filter_col, filter_val, expected_count):
    df = spark_fixture.createDataFrame(data, ["year", "profit"])
    filtered = df.filter(f"{filter_col}={filter_val}")
    assert filtered.count() == expected_count

@pytest.mark.parametrize("data,group_col,expected_count", [
    ([(1, 100)] * 100 + [(2, 50)], "customer_id", 1)
])
# Test detection of skewed groups (count > 50)
def test_skew_detection(spark_fixture, data, group_col, expected_count):
    df = spark_fixture.createDataFrame(data, ["customer_id", "profit"])
    skew = df.groupBy(group_col).count().filter("count>50")
    assert skew.count() == expected_count

@pytest.mark.parametrize("data,drop_col,expected_count", [
    ([(1, 100), (1, 100)], "order_id", 1)
])
# Test idempotent logic for dropping duplicates
def test_idempotent_logic(spark_fixture, data, drop_col, expected_count):
    df = spark_fixture.createDataFrame(data, ["order_id", "profit"])
    result = df.dropDuplicates([drop_col])
    assert result.count() == expected_count

# Edge case: test schema with empty DataFrame
def test_schema_empty_df(spark_session):
    df = spark_session.createDataFrame([], "id INT, name STRING")
    assert "id" in df.columns
    assert "name" in df.columns

# Edge case: test rounding with negative values
@pytest.mark.parametrize("input_value,expected", [
    (-10.456, -10.46),
    (-0.999, -1.0)
])
def test_rounding_negative(spark_session, input_value, expected):
    df = spark_session.createDataFrame([(1, input_value)], ["id", "profit"])
    result = df.withColumn("profit", round("profit", 2))
    assert pytest.approx(result.collect()[0]["profit"], 0.01) == expected

# Test detection of no duplicates in DataFrame
def test_duplicate_detection_no_duplicates(spark_session):
    df = spark_session.createDataFrame([(1,), (2,)], ["order_id"])
    dup = df.groupBy("order_id").count().filter("count>1")
    assert dup.count() == 0

# Test idempotent logic with empty DataFrame
def test_idempotent_logic_empty(spark_fixture):
    df = spark_fixture.createDataFrame([], "order_id INT, profit INT")
    result = df.dropDuplicates(["order_id"])
    assert result.count() == 0

@pytest.mark.parametrize("data,filter_col,filter_val,expected_count", [
    ([(2024, 100), (2023, 50)], "year", 2025, 0)
])
# Test filtering DataFrame with no matching partition value
def test_partition_filtering_no_match(spark_fixture, data, filter_col, filter_val, expected_count):
    df = spark_fixture.createDataFrame(data, ["year", "profit"])
    filtered = df.filter(f"{filter_col}={filter_val}")
    assert filtered.count() == expected_count

@pytest.fixture
def spark_fixture(spark):
    return spark

@pytest.mark.parametrize("data,expected_types", [
    ([(1, "Alice", 3.14)], {"id": "int", "name": "string", "score": "double"})
])
# Test column data types in DataFrame schema
def test_column_types(spark_fixture, data, expected_types):
    df = spark_fixture.createDataFrame(data, ["id", "name", "score"])
    schema = dict(df.dtypes)
    for col, dtype in expected_types.items():
        assert schema[col] == dtype

@pytest.mark.parametrize("data,expected_null_count", [
    ([(1, None), (2, "Bob")], 1),
    ([(1, "Alice"), (2, "Bob")], 0)
])
# Test handling of null values in DataFrame
def test_null_handling(spark_fixture, data, expected_null_count):
    df = spark_fixture.createDataFrame(data, ["id", "name"])
    null_count = df.filter("name IS NULL").count()
    assert null_count == expected_null_count

@pytest.mark.parametrize("data,filter_expr,expected_count", [
    ([(1, 100), (2, 50), (3, 100)], "profit=100 AND id=1", 1),
    ([(1, 100), (2, 50), (3, 100)], "profit=50", 1)
])
# Test filtering DataFrame with multiple conditions
def test_filter_multiple_conditions(spark_fixture, data, filter_expr, expected_count):
    df = spark_fixture.createDataFrame(data, ["id", "profit"])
    filtered = df.filter(filter_expr)
    assert filtered.count() == expected_count

@pytest.mark.parametrize("data,drop_col", [
    ([(1, "Alice", 3.14)], "score")
])
# Test