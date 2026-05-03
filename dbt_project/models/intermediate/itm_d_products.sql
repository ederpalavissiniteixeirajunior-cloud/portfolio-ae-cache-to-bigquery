with staging as (
    select * from {{ ref('stg_products') }}
),

intermediate as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['cd_product']) }} as sk_product,
        cd_product,
        upper(ds_product) as ds_product,
        regexp_extract(ds_product, r'(\d{3}\.\d{2}\.\d{5})') as ds_product_reference,
        ds_color,
        current_timestamp() as updated_at
    from staging
    where 
        1=1
        AND cd_company = 1
        AND ds_entry_mask03 LIKE '%COLCCI'
        AND ds_entry_mask01 LIKE '8%'
)

select * from intermediate