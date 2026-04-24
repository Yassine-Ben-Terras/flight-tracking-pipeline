{% macro meters_to_feet(column_name, decimal_places=2) %}
    round( ({{ column_name }} * 3.28084)::numeric, {{ decimal_places }} )
{% endmacro %}

{% macro mps_to_knots(column_name, decimal_places=2) %}
    round( ({{ column_name }} * 1.94384)::numeric, {{ decimal_places }} )
{% endmacro %}