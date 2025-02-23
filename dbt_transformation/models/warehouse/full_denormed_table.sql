{{config(materialized = 'view')}}
WITH base_sales AS (
    SELECT DISTINCT 
        fs.saleshistorykey, 
        fs.realestatepropertycode, 
        fs.saleamt,
        fs.salestypekey,
        fs.saledatekey
    FROM {{ ref("fact_sales") }} fs
)

SELECT 
    bs.saleshistorykey, 
    bs.realestatepropertycode, 
    SUBSTRING(st.salestypedsc FROM 3) AS salestypedsc,
    sd.saledate,
    bs.saleamt,
    p.propertyStreetNbrNameText,
    SUBSTRING(pc.propertyclasstypedsc FROM 4) AS propertyclasstypedsc,
    dg.coolingtypedsc,
    dg.dwellingtypedsc,
    dg.heatingtypedsc,
    dg.dwellingyearbuiltdate,
    dg.storiesquantitydsc,
    dg.fireplacecnt,
    di.bedroomcnt,
    di.bathroomcnt
    --ob.outbuildingtypedsc,
    --ob.outbuildingsquarefeetqty
FROM base_sales bs
JOIN {{ ref("dim_sales_type") }} st ON bs.salestypekey = st.salestypekey
JOIN {{ ref("dim_sale_date") }} sd ON bs.saledatekey = sd.saledatekey
JOIN {{ ref("dim_property") }} p ON bs.realestatepropertycode = p.realestatepropertycode
JOIN {{ ref("dim_property_class") }} pc ON p.propertyclasstypekey = pc.propertyclasstypekey
JOIN {{ ref("dim_dwellings_general") }} dg ON bs.realestatepropertycode = dg.realestatepropertycode
JOIN {{ dwelling_interior_info() }} di ON bs.realestatepropertycode = di.realestatepropertycode
--JOIN {{ ref("dim_outbuildings") }} ob ON bs.realestatepropertycode = ob.realestatepropertycode
WHERE sd.saledate <= current_date and bs.saleamt > 0






       