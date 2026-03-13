
import sys, os

PROJECT_ROOT = "/Workspace/Users/s.karthikeyan1100@gmail.com/dataprojects/PEI-Databricks-project"

sys.path.append(PROJECT_ROOT)
from src.loaders.data_loader import load_data
from src.utils.config import *

df = load_data(CUSTOMERS_PATH,"excel")
df.write.format("delta").mode("overwrite").saveAsTable(RAW_CUSTOMERS)
