# load raw table

from pyspark.sql.functions import col, regexp_replace
from delta.tables import DeltaTable
raw_customers = spark.table("raw_customers")

enriched_customers = raw_customers.select(
    col("customer_id"),
    regexp_replace(col("customer_name"), r"[^a-zA-Z0-9\s]", "").alias("customer_name"),
    col("segment"),
    col("country"),
    col("city"),
    col("state")
)
if spark.catalog.tableExists("enriched_customers"):
    delta_table = DeltaTable.forName(spark, "enriched_customers")
    delta_table.alias("target").merge(
        enriched_customers.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    enriched_customers.write \
        .format("delta") \
        .saveAsTable("enriched_customers")