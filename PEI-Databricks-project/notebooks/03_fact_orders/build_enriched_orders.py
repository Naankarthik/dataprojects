
import sys, os
from delta.tables import DeltaTable
PROJECT_ROOT = "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"
sys.path.append(PROJECT_ROOT)
from src.transformations.order_transformations import build_enriched_orders
from src.utils.config import *

orders = spark.table(RAW_ORDERS)
customers = spark.table(ENRICHED_CUSTOMERS)
products = spark.table(ENRICHED_PRODUCTS)

enriched = build_enriched_orders(orders,customers,products).dropDuplicates(['order_id'])
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
    enriched.write.format("delta").saveAsTable(ENRICHED_ORDERS)
