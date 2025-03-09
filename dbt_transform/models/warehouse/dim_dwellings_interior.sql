SELECT * FROM {{ source('postgres', 'dim_dwellings_interior')}}
