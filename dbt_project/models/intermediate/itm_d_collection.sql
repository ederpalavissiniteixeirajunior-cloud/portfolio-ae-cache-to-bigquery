
with staging as (
    select * from {{ ref('stg_collection') }}
),

intermediate as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_collection']) }} as sk_collection,
        id_collection,
        collection_name,
        start_date,
        end_date,
        SAFE_SUBTRACT(id_collection, 4) as id_collection_ly,
        "Integração GEOvendas" as operation_filter,
        current_timestamp() as updated_at
    from staging
)

select * from intermediate