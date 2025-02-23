SELECT * FROM {{ source('postgres_db', 'dim_outbuildings')}}
WHERE realestatepropertycode in (
    SELECT DISTINCT realestatepropertycode FROM {{ source('postgres_db', 'dim_outbuildings')}}
)