with source as (
    select * from {{ ref('itm_d_collection') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sk_collection']) }} as sk_collection_version,
        sk_collection, 
        id_collection,
        collection_name,
        start_date,
        end_date,
        id_collection_ly,
        operation_filter,
        current_timestamp() as updated_at
        from source
)

select * from final