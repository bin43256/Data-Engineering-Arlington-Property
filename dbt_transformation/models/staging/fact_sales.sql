WITH unique_sales AS (
    SELECT DISTINCT saleshistorykey
    FROM {{ source('postgres_db', 'fact_sales')}}
)

SELECT 
    us.saleshistorykey,
    fs.realestatepropertycode,
    fs.propertykey,
    fs.salestypekey,
    fs.saledatekey,
    fs.saleamt
FROM unique_sales us
JOIN {{ source('postgres_db', 'fact_sales')}} fs 
    ON us.saleshistorykey = fs.saleshistorykey