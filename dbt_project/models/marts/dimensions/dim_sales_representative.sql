with source as (
    select * from {{ ref('snp_d_sales_representative') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sk_sales_representative', 'dbt_updated_at']) }} as sk_sales_representative_version,
        sk_sales_representative, 
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
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current
    from source
)

select * from final