📊 Retail Sales Performance Tracker
This project demonstrates an end-to-end analytics workflow using the Superstore retail sales dataset.
The goal is to model and analyze retail performance metrics such as sales, profit, order volume, and shipping efficiency across regions, categories, and customer segments.
The solution uses Snowflake as the cloud data warehouse, dbt for data modeling, and Streamlit to build an interactive dashboard.
✅ Project Overview
The dataset contains historical transactional sales data, including:
Order + shipment dates
Customer + segment details
Product category + sub-category
Region + geography
Metrics: sales, quantity, profit, discount
This project focuses on:
✔ Cleaning + standardizing raw data
✔ Building fact models for business analysis
✔ Providing insights through an interactive dashboard
🏗️ Architecture
Raw Data  →  Staging (dbt)  →  Fact Models (dbt)  →  Streamlit Dashboard
🔹 Technologies
Tool	Purpose
Snowflake	Cloud data warehouse
dbt	SQL modeling, testing, lineage
Streamlit	Interactive dashboard
Python	Snowflake connectivity
Git & GitHub	Version control
📦 Data Modeling (dbt)
✅ Models Used
Model	Description
stg_orders	Cleans + standardizes raw sales data
fct_sales	Core sales transaction table
fct_sales_by_category	Aggregated metrics by category + sub-category
fct_sales_by_region	Aggregated metrics by region
fct_sales_by_segment	Aggregated metrics by customer segment
fct_ship_time	Shipping duration + shipping KPIs
Note: Dimension and additional fact models were intentionally removed to keep the project minimal and focused.
Running dbt
Install dbt-snowflake:
pip install dbt-snowflake
Configure ~/.dbt/profiles.yml with your Snowflake credentials, then:
dbt debug      # validate connection
dbt run        # build models
dbt test       # run column tests
dbt docs serve # optional lineage + model docs
📊 Streamlit Dashboard
The dashboard queries Snowflake fact tables to display:
✅ KPIs
Total Sales
Total Profit
Total Orders
✅ Charts + Views
Monthly sales trend
Category + sub-category performance
Regional performance
Customer segment analytics
Shipping time benchmarks
✅ Filters
Region
Category
🚀 Running the Dashboard
1) Install dependencies
cd streamlit_dashboard
pip install -r requirements.txt
2) Configure Environment
Create .env inside streamlit_dashboard/:
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=xxxx
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=STAGING
.env is ignored by Git.
A template .env.example is provided.
3) Start Streamlit
streamlit run app.py
Open:
http://localhost:8501
📁 Project Structure
.
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   ├── marts/
│   │   ├── fct_sales.sql
│   │   ├── fct_sales_by_category.sql
│   │   ├── fct_sales_by_region.sql
│   │   ├── fct_sales_by_segment.sql
│   │   └── fct_ship_time.sql
│   ├── staging.yml
│   └── marts.yml
│
├── streamlit_dashboard/
│   ├── app.py
│   ├── main.py
│   ├── .env.example
│   └── requirements.txt
│
├── dbt_project.yml
├── .gitignore
└── README.md
