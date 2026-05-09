with staging as (
    select * from {{ ref('stg_sales_representative') }}
),

intermediate as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['cd_sales_representative']) }} as sk_sales_representative,
        cd_sales_representative,
        nk_sales_representatives,
        id_sales_representative_uuid,
        ds_representative_name,
        cd_tax_id,
        ds_email,
        ds_city,
        ds_state,
        ds_country,
        ds_region,
        cd_ibge_city,
        ds_geo_location,
        pc_commission_default,
        pc_commission_invoiced,
        pc_commission_paid,
        cd_commission_type,
        ds_commission_type,
        cd_status,
        ds_status,
        current_timestamp() as updated_at
    from staging
    where cd_company = 1
)

select * from intermediate