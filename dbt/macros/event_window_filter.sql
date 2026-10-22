{% macro event_window_filter(date_column) -%}
    {%- set event_date = var("event_date", none) -%}
    {%- if event_date is not none and (event_date | string) not in ["", "None", "null"] -%}
    and {{ date_column }} = cast('{{ event_date }}' as date)
    {%- elif var("dev_mode", false) -%}
    and {{ date_column }} >= cast('{{ var("start_date") }}' as date)
    and {{ date_column }} < cast('{{ var("start_date") }}' as date) + interval '7' day
    {%- endif -%}
{%- endmacro %}
