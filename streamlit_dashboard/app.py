# streamlit_dashboard/app.py
import os
import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

# ----------------------------
# Setup
# ----------------------------
load_dotenv()
st.set_page_config(page_title="Retail Sales Dashboard", layout="wide")
st.title("📊 Retail Sales Performance Dashboard")

# ----------------------------
# Snowflake helpers
# ----------------------------
def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "STAGING"),
    )

@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    cn = get_conn()
    cur = cn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()
        cn.close()

def qlist(vals: list[str]) -> str:
    """Quote + escape values safely for IN (...) clauses."""
    if not vals:
        return ""
    return ", ".join(["'{}'".format(v.replace("'", "''")) for v in vals])

# ----------------------------
# Sidebar filters
# ----------------------------
st.sidebar.header("Filters")

regions_df = run_query("SELECT DISTINCT region FROM fct_sales ORDER BY region")
cats_df    = run_query("SELECT DISTINCT category FROM fct_sales ORDER BY category")

regions = regions_df["REGION"].dropna().tolist()
cats    = cats_df["CATEGORY"].dropna().tolist()

sel_regions    = st.sidebar.multiselect("Region", regions)
sel_categories = st.sidebar.multiselect("Category", cats)

# WHERE snippets
region_where = "WHERE 1=1"
if sel_regions:
    region_where += f"\n  AND region IN ({qlist(sel_regions)})"

category_where = "WHERE 1=1"
if sel_categories:
    category_where += f"\n  AND category IN ({qlist(sel_categories)})"

both_where = "WHERE 1=1"
if sel_regions:
    both_where += f"\n  AND region IN ({qlist(sel_regions)})"
if sel_categories:
    both_where += f"\n  AND category IN ({qlist(sel_categories)})"

row_where = both_where  # for FCT_SALES (has both)

# ----------------------------
# Tabs
# ----------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🏠 Overview", "📦 Category", "🌍 Region", "👥 Segment", "🚚 Shipping"]
)

# ----------------------------
# TAB 1 – OVERVIEW
# ----------------------------
with tab1:
    st.subheader("Overall KPIs")

    kpi = run_query(f"""
        SELECT
          SUM(sales)               AS total_sales,
          SUM(profit)              AS total_profit,
          COUNT(DISTINCT order_id) AS total_orders
        FROM fct_sales
        {row_where}
    """)

    c1, c2, c3 = st.columns(3)
    if not kpi.empty:
        c1.metric("Total Sales",  f"${kpi['TOTAL_SALES'][0]:,.0f}")
        c2.metric("Total Profit", f"${kpi['TOTAL_PROFIT'][0]:,.0f}")
        c3.metric("Total Orders", f"{int(kpi['TOTAL_ORDERS'][0]):,}")
    else:
        c1.metric("Total Sales", "$0")
        c2.metric("Total Profit", "$0")
        c3.metric("Total Orders", "0")

    st.markdown("---")
    st.subheader("Monthly Sales Trend")

    monthly = run_query(f"""
        SELECT
          DATE_TRUNC('month', order_date) AS period,
          SUM(sales)                      AS total_sales
        FROM fct_sales
        {row_where}
        GROUP BY 1
        ORDER BY 1
    """)
    if not monthly.empty:
        st.line_chart(monthly, x="PERIOD", y="TOTAL_SALES")
    else:
        st.info("No data for the selected filters.")

# ----------------------------
# TAB 2 – CATEGORY
# ----------------------------
with tab2:
    st.subheader("Category & Sub-category Performance")

    df_cat = run_query(f"""
        SELECT category, sub_category, total_sales, total_profit, profit_margin
        FROM fct_sales_by_category
        {category_where}
        ORDER BY category, sub_category
    """)
    if not df_cat.empty:
        st.bar_chart(
            df_cat.groupby("CATEGORY", as_index=False)["TOTAL_SALES"].sum(),
            x="CATEGORY", y="TOTAL_SALES"
        )
        with st.expander("Details"):
            st.dataframe(df_cat.sort_values(["TOTAL_SALES"], ascending=False))
    else:
        st.info("No data for the selected filters.")

# ----------------------------
# TAB 3 – REGION
# ----------------------------
with tab3:
    st.subheader("Regional Performance")

    df_reg = run_query(f"""
        SELECT region, orders, total_sales, total_profit, profit_margin
        FROM fct_sales_by_region
        {region_where}
        ORDER BY region
    """)
    if not df_reg.empty:
        st.bar_chart(df_reg, x="REGION", y="TOTAL_SALES")
        with st.expander("Details"):
            st.dataframe(df_reg.sort_values(["TOTAL_SALES"], ascending=False))
    else:
        st.info("No data for the selected filters.")

# ----------------------------
# TAB 4 – SEGMENT
# ----------------------------
with tab4:
    st.subheader("Customer Segment Performance")

    df_seg = run_query("""
        SELECT segment, orders, total_sales, total_profit, profit_margin, avg_order_value
        FROM fct_sales_by_segment
        ORDER BY total_sales DESC
    """)
    if not df_seg.empty:
        st.bar_chart(df_seg, x="SEGMENT", y="TOTAL_SALES")
        with st.expander("Details"):
            st.dataframe(df_seg)
    else:
        st.info("No data available.")

# ----------------------------
# TAB 5 – SHIPPING
# ----------------------------
with tab5:
    st.subheader("Shipping KPIs by Region / Category")

    df_ship = run_query(f"""
        SELECT region, category, sub_category,
               orders, avg_days_to_ship, p50_days_to_ship, p90_days_to_ship,
               total_sales, total_profit
        FROM fct_ship_time
        {both_where}
        ORDER BY region, category, sub_category
    """)
    if not df_ship.empty:
        st.dataframe(df_ship.sort_values(["AVG_DAYS_TO_SHIP"]))
    else:
        st.info("No data for the selected filters.")
