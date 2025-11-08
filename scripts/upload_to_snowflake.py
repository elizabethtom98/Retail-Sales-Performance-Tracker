import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# 1) Load env vars
load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# 2) Read CSV (handle encoding + parse dates)
#csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "Superstore.csv")
df = pd.read_csv(csv_path, encoding="ISO-8859-1")

# Optional: align column names to Snowflake table (strip spaces, consistent casing)
df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

# Map common Superstore headers to our Snowflake schema
rename_map = {
    "Row_ID": "ROW_ID",
    "Order_ID": "ORDER_ID",
    "Order_Date": "ORDER_DATE",
    "Ship_Date": "SHIP_DATE",
    "Ship_Mode": "SHIP_MODE",
    "Customer_ID": "CUSTOMER_ID",
    "Customer_Name": "CUSTOMER_NAME",
    "Segment": "SEGMENT",
    "Country": "COUNTRY",
    "City": "CITY",
    "State": "STATE",
    "Postal_Code": "POSTAL_CODE",
    "Region": "REGION",
    "Product_ID": "PRODUCT_ID",
    "Category": "CATEGORY",
    "Sub_Category": "SUB_CATEGORY",
    "Product_Name": "PRODUCT_NAME",
    "Sales": "SALES",
    "Quantity": "QUANTITY",
    "Discount": "DISCOUNT",
    "Profit": "PROFIT",
}
df = df.rename(columns=rename_map)

# Coerce types
for col in ["SALES", "PROFIT", "DISCOUNT"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

for col in ["QUANTITY", "ROW_ID"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

for col in ["ORDER_DATE", "SHIP_DATE"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

# Reorder/select only the columns that exist in Snowflake table
target_cols = [
    "ROW_ID","ORDER_ID","ORDER_DATE","SHIP_DATE","SHIP_MODE","CUSTOMER_ID","CUSTOMER_NAME",
    "SEGMENT","COUNTRY","CITY","STATE","POSTAL_CODE","REGION","PRODUCT_ID","CATEGORY",
    "SUB_CATEGORY","PRODUCT_NAME","SALES","QUANTITY","DISCOUNT","PROFIT"
]
df = df[[c for c in target_cols if c in df.columns]]

# 3) Load via write_pandas (fast, uses Snowflake bulk copy underneath)
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df,
    table_name="ORDERS",
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    quote_identifiers=True,  # handles reserved words safely
    overwrite=False          # append (set True to replace)
)

print(f"✅ write_pandas success={success}, chunks={nchunks}, rows_loaded={nrows}")

# 4) Quick verification queries
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM RAW.ORDERS;")
print("Total rows now in RAW.ORDERS:", cur.fetchone()[0])

cur.execute("""
    SELECT REGION, ROUND(SUM(SALES),2) AS SALES, ROUND(SUM(PROFIT),2) AS PROFIT
    FROM RAW.ORDERS
    GROUP BY REGION
    ORDER BY SALES DESC;
""")
print("\nSales by Region (top first):")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
print("✅ Done.")
