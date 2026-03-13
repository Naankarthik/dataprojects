import sys, os

PROJECT_ROOT = "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"
sys.path.append(PROJECT_ROOT)
from src.transformations.aggregation_transformations import build_profit_aggregation
from src.utils.config import *


orders = spark.table(ENRICHED_ORDERS)

agg = build_profit_aggregation(orders)
# agg.display()
agg.write.format("delta").mode("overwrite").saveAsTable(AGG_PROFIT)
