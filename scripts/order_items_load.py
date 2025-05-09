import os
import json
import pandas as pd
import snowflake.connector
from base_loader import BaseLoader

class OrderItemsLoader(BaseLoader):
    def __init__(self, connector):
        self.conn = connector
        self.data_file = os.path.expanduser("~/Projects/retail-sales-analytics-pipeline/data/order_items.csv")
        self.schema_file = os.path.expanduser("~/Projects/retail-sales-analytics-pipeline/config/schema_order_items.json")
        self.table = "LND_ORDER_ITEMS"
        self.schema = "RETAIL_LANDING"
        self.file_type = "csv"

    def validate_and_load_orderitems_file(self):
        try:
            loader = BaseLoader()
            df = loader.load_file(self.data_file, self.schema_file, self.file_type)
            loader.insert_into_snowflake(self.conn, df, self.table, self.schema)
        except Exception as e:
            print(f"Error {e}")