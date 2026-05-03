{{ config(materialized='table') }}

with source as (
    select * from {{ ref('snp_d_products') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sk_product', 'dbt_updated_at']) }} as sk_product_version,
        sk_product, 
        cd_product, 
        ds_product,
        ds_product_reference, 
        ds_color,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current
    from source
)

select * from final