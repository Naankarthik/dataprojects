
from src.loaders.data_loader import load_data
from src.utils.config import *

df = load_data(ORDERS_PATH,"json")
df.write.format("delta").mode("overwrite").saveAsTable(RAW_ORDERS)
