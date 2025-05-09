import snowflake.connector
from dotenv import load_dotenv
import os

class connect_to_snowflake():
    @staticmethod
    def get_connection():
        load_dotenv(dotenv_path=os.path.expanduser("~/Projects/retail-sales-analytics-pipeline/config/snowflake.env"),  override=True)
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SF_USER"),
                password=os.getenv("SF_PASSWORD"),
                account=os.getenv("SF_ACCOUNT"),
                warehouse=os.getenv("SF_WAREHOUSE"),
                database=os.getenv("SF_DATABASE"),
                schema=os.getenv("SF_SCHEMA"),
                role=os.getenv("SF_ROLE")
            )
            print("✅ Connected to Snowflake")
            return conn
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")