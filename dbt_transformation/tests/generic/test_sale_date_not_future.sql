{% test sale_date_not_future(model, column_name) %}

with validation as (
    select 
        saleDate
    from {{ model }}
    where saleDate > current_date
)

select *
from validation

{% endtest %} 