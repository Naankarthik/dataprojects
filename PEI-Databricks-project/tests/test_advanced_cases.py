def test_partition_filtering(spark):

    df = spark.createDataFrame([(2024,100),(2023,50)],["year","profit"])
    filtered = df.filter("year=2024")

    assert filtered.count()==1

def test_skew_detection(spark):

    data = [(1,100)]*100 + [(2,50)]
    df = spark.createDataFrame(data,["customer_id","profit"])

    skew = df.groupBy("customer_id").count().filter("count>50")

    assert skew.count()==1

def test_idempotent_logic(spark):

    df = spark.createDataFrame([(1,100),(1,100)],["order_id","profit"])
    result = df.dropDuplicates(["order_id"])

    assert result.count()==1
