import os
import pandas as pd
import json
from snowflake.connector import *
from snowflake.connector.pandas_tools import write_pandas

class BaseLoader:
    def __init__(self):
        None

    def load_file(self, data_file, schema_file, file_type):
        self.data_file = data_file
        self.schema_file = schema_file
        self.file_type = file_type

        if file_type == "json":
            data = pd.read_json(self.data_file)
            df = pd.DataFrame(data)
        elif file_type == "csv":
            data = pd.read_csv(self.data_file)
            df = pd.DataFrame(data)
        elif file_type == "xml":
            data = pd.read_xml(self.data_file)
            df = pd.DataFrame(data)
        else:
            print("error")
            
        with open(self.schema_file, 'r') as f:
            schema = json.load(f)
            
        try:
            df = df.astype(schema)
        except Exception as e:
            print(f"Data doesnt confirm to schema: {e}")
            
        return df
        
    def insert_into_snowflake(self, connector, df, table, schema):
        self.conn = connector
        self.df = df
        self.table = table
        self.schema = schema
        
        success, nchunks, nrows, output = write_pandas(
            conn=self.conn,
            df=self.df,
            table_name=table,
            schema=schema  
        )

        print(f"{success}, {nrows} loaded into table {self.table}")
        