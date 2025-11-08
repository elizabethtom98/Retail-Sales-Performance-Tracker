{{ config(materialized='view') }}  -- creates a VIEW in STAGING by default

select
    ROW_ID,
    ORDER_ID,
    ORDER_DATE,
    SHIP_DATE,
    SHIP_MODE,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    SEGMENT,
    COUNTRY,
    CITY,
    STATE,
    POSTAL_CODE,
    REGION,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT
from {{ source('raw','orders') }}
