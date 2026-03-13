from pyspark.sql.functions import col
from delta.tables import DeltaTable

raw_products = spark.table("raw_products")
enriched_products = raw_products.select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("sub-category").alias("sub_category")
)
enriched_products=enriched_products.dropDuplicates(['product_id'])

if spark.catalog.tableExists("enriched_products"):
    delta_table = DeltaTable.forName(spark, "enriched_products")
    delta_table.alias("target").merge(
        enriched_products.alias("source"),
        "target.product_id = source.product_id"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    enriched_products.write \
        .format("delta") \
        .saveAsTable("enriched_products")
