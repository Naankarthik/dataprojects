import sys, os

PROJECT_ROOT = "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"
sys.path.append(PROJECT_ROOT)

from src.transformations.aggregation_transformations import build_profit_aggregation
from src.utils.config import *
#from src.utils.spark_optimizations import set_spark_performance_configs

#set_spark_performance_configs(spark)  ## not working in serveless cluster


orders = spark.table(ENRICHED_ORDERS)

agg = build_profit_aggregation(orders)
# agg.display()
if spark.catalog.tableExists(AGG_PROFIT):
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forName(spark, AGG_PROFIT)
    delta_table.alias("t").merge(
        agg.alias("s"),
        "t.year = s.year AND t.category = s.category AND t.sub_category = s.sub_category AND t.customer_name = s.customer_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    (
        agg.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("year", "category", "sub_category")
        .saveAsTable(AGG_PROFIT)
    )
