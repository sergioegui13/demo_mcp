{{ config(
    materialized = 'table',
    tags = ['silver']
) }}

with source as (
    select *
    from {{ source('tpch','nation') }}
)

, renamed as (
    select
        cast({{ dbt_utils.generate_surrogate_key(['c_custkey']) }} as varchar(32)) as id_customer
        , cast({{ dbt_utils.generate_surrogate_key(['c_nationkey']) }} as varchar(32)) as id_nation
        , cast(n_nationkey as number) as nation_key
        , cast(n_regionkey as number) as region_key
        , trim(n_name) as nation_name
        , trim(n_comment) as nation_comment
        , loaded_at as loaded_at_utc
        , convert_timezone('UTC', current_timestamp()) as staged_at_utc
        , '{{ invocation_id }}' as dbt_invocation_id
        , '{{ env_var("DBT_CLOUD_RUN_ID","local") }}' as dbt_cloud_run_id
        , '{{ env_var("DBT_CLOUD_JOB_ID","manual") }}' as dbt_cloud_job_id
    from source
    where
        n_nationkey is not null
        and n_name is not null
        and n_regionkey is not null
)

select *
from renamed
