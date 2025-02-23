SELECT * FROM {{ source('postgres_db', 'dim_dwellings_general')}}
WHERE realestatepropertycode in (
    SELECT DISTINCT realestatepropertycode FROM {{ source('postgres_db', 'dim_dwellings_general')}}
)