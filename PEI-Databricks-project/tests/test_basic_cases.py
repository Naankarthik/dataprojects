
from pyspark.sql.functions import round

def test_schema(spark):
    df = spark.createDataFrame([(1,"John")],["id","name"])
    assert "id" in df.columns

def test_not_empty(spark):
    df = spark.createDataFrame([(1,)],["id"])
    assert df.count()>0

def test_empty_df(spark):
    df = spark.createDataFrame([], "id INT")
    assert df.count()==0

def test_rounding(spark):
    df = spark.createDataFrame([(1,10.456)],["id","profit"])
    result = df.withColumn("profit",round("profit",2))
    assert result.collect()[0]["profit"] == 10.46

def test_duplicate_detection(spark):
    df = spark.createDataFrame([(1,),(1,)],["order_id"])
    dup = df.groupBy("order_id").count().filter("count>1")
    assert dup.count()==1
