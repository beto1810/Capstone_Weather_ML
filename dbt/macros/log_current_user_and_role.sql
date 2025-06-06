{% macro log_current_user_and_role() %}
  {% set user_result = run_query("SELECT CURRENT_USER()") %}
  {% set role_result = run_query("SELECT CURRENT_ROLE()") %}

  {% if user_result is not none and role_result is not none %}
    {% set user = user_result.columns[0].values()[0] %}
    {% set role = role_result.columns[0].values()[0] %}
    {% do log("User: " ~ user, info=True) %}
    {% do log("Role: " ~ role, info=True) %}
  {% else %}
    {% do log("⚠️ Warning: user or role result is None", info=True) %}
  {% endif %}
{% endmacro %}