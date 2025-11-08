import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

print("✅ Connection established!")

# Run a simple SQL query
cur = conn.cursor()
cur.execute("SELECT CURRENT_USER(), CURRENT_REGION(), CURRENT_VERSION();")

for row in cur:
    print(row)

cur.close()
conn.close()
print("✅ Connection closed.")
