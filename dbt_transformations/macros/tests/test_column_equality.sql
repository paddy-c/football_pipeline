{% test column_equality(model, column1, column2) %}
    select *
    from {{ model }}
    where cast({{ column1 }} as INT) != cast({{ column2 }} as INT)
    and {{ column1 }} IS NOT NULL 
    and {{ column2 }} IS NOT NULL
{% endtest %}