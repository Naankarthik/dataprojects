
import sys, os
from delta.tables import DeltaTable

PROJECT_ROOT = "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"
sys.path.append(PROJECT_ROOT)
from src.transformations.order_transformations import build_enriched_orders
from src.utils.config import *
#from src.utils.spark_optimizations import set_spark_performance_configs

#set_spark_performance_configs(spark)  ## not working in serveless cluster
 
orders = spark.table(RAW_ORDERS).dropDuplicates(["order_id"])
customers = spark.table(ENRICHED_CUSTOMERS).dropDuplicates(["customer_id"])
products = spark.table(ENRICHED_PRODUCTS).dropDuplicates(["product_id"])
enriched = build_enriched_orders(
    orders,
    customers,
    products,
    repartition_col="customer_id"
).dropDuplicates(["order_id"])
# enriched.display()

if spark.catalog.tableExists(ENRICHED_ORDERS):
    _table = DeltaTable.forName(spark, ENRICHED_ORDERS)
    _table.alias("target").merge(
        enriched.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    (
    enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("customer_id", "product_id", "order_date_parsed")
    .saveAsTable(ENRICHED_ORDERS)
)
