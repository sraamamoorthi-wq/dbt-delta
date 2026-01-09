{% docs materialization_custom_incremental %}

### Custom Incremental Materialization

**Purpose:**
This materialization handles incremental loads using a "delete+insert" strategy...

**Required Arguments:**
* `unique_key`: The primary key used to identify records for deletion.

**Usage:**
Add this config block to the top of your model:

```sql
{% raw %}
{{ config(
    materialized='custom_incremental',
    unique_key='transaction_id'
) }}
{% endraw %}
{% enddocs %}