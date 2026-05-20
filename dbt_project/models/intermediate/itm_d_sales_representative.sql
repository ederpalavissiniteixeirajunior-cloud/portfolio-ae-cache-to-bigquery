with staging as (
    select * from {{ ref('stg_sales_representative') }}
),

intermediate as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_sales_representative']) }} as sk_sales_representative,
        cd_sales_representative,
        nk_sales_representatives,
        id_sales_representative_uuid,
        ds_representative_name,
        case
            when CAST(cd_sales_representative AS STRING) in ('190','195') then "Sales Rep 01"
            when CAST(cd_sales_representative AS STRING) in ('9') then "Sales Rep 02"
            when CAST(cd_sales_representative AS STRING) in ('8') then "Sales Rep 03"
            when CAST(cd_sales_representative AS STRING) in ('44','175','244') then "Sales Rep 04"
            when CAST(cd_sales_representative AS STRING) in ('11','196') then "Sales Rep 05"
            when CAST(cd_sales_representative AS STRING) in ('10','90','119') then "Sales Rep 06"
            when CAST(cd_sales_representative AS STRING) in ('5','500') then "Sales Rep 07"
            when CAST(cd_sales_representative AS STRING) in ('48') then "Sales Rep 08"
            when CAST(cd_sales_representative AS STRING) in ('150') then "Sales Rep 09"
            when CAST(cd_sales_representative AS STRING) in ('13','169') then "Sales Rep 10"
            when CAST(cd_sales_representative AS STRING) in ('7','209') then "Sales Rep 11"
            when CAST(cd_sales_representative AS STRING) in ('251') then "Sales Rep 12"
            when CAST(cd_sales_representative AS STRING) in ('12') then "Sales Rep 13"
            when CAST(cd_sales_representative AS STRING) in ('3','250') then "Sales Rep 14"
            when CAST(cd_sales_representative AS STRING) in ('217','249') then "Sales Rep 15"
            when CAST(cd_sales_representative AS STRING) in ('255') then "Sales Rep 16"
            when CAST(cd_sales_representative AS STRING) in ('998') then "Sales Rep 17"
            when CAST(cd_sales_representative AS STRING) in ('999') then "Sales Rep 18"
            when CAST(cd_sales_representative AS STRING) in ('260') then "Sales Rep 19"
            when CAST(cd_sales_representative AS STRING) in ('261') then "Sales Rep 20"
            when CAST(cd_sales_representative AS STRING) in ('259') then "Sales Rep 21"
            when CAST(cd_sales_representative AS STRING) in ('270') then "Sales Rep 22"
            when CAST(cd_sales_representative AS STRING) in ('118') then "Sales Rep 23"
            when CAST(cd_sales_representative AS STRING) in ('271') then "Sales Rep 24"
            when CAST(cd_sales_representative AS STRING) in ('268') then "Sales Rep 25"
            when CAST(cd_sales_representative AS STRING) in ('269') then "Sales Rep 26"
            else "-"
            end as nm_group_sales_representative,            
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
where nm_group_sales_representative <> "-"