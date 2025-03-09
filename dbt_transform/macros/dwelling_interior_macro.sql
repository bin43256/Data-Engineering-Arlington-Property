{% macro dwelling_interior_info() %}
(SELECT realestatepropertycode,
       sum(bedroomcnt) as bedroomcnt,
       sum(twofixturebathroomcnt + 
           threefixturebathroomcnt + 
           fourfixturebathroomcnt + 
           fivefixturebathroomcnt) as bathroomcnt
FROM {{ref("dim_dwellings_interior")}}
GROUP BY realestatepropertycode)
{% endmacro %}