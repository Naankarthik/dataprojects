
from src.loaders.data_loader import load_data
from src.utils.config import *

df = load_data(PRODUCTS_PATH,"csv")
df.write.format("delta").mode("overwrite").saveAsTable(RAW_PRODUCTS)
