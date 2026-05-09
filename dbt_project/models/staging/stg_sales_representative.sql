-- models/staging/stg_sales_representatives.sql

with source as (
    select * from {{ source('raw_cache', 'representantes') }}
),

renamed as (
    select
        -- Keys
        cast(ID as string) as nk_sales_representatives,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as cd_sales_representative,
        cast(idRepresentante as string) as id_sales_representative_uuid,
        cast(codEmpresa as int64) as cd_company,

        -- PII Masked Info
        {{ mask_pii_info('nome','name') }} as ds_representative_name,
        {{ mask_pii_info('CNPJCPF') }} as cd_tax_id,
        {{ mask_pii_info('emailRepresentante','email') }} as ds_email,

        --Geography Info
        cast(cidade as string) as ds_city,
        cast(estado as string) as ds_state,
        cast(pais as string) as ds_country,
        cast(regiao as string) as ds_region,
        cast(codIBGE as string) as cd_ibge_city,
        cast(geoRepresentante as string) as ds_geo_location,

        --Commission Rules
        cast(percComissao as float64) as pc_commission_default,
        cast(percComissaoFaturamento as float64) as pc_commission_invoiced,
        cast(percComissaoLiquidacao as float64) as pc_commission_paid,
        cast(codTipoComissao as string) as cd_commission_type,
        cast(tipoComissao as string) as ds_commission_type,

        --Qualitative Info
        cast(codSituacao as string) as cd_status,
        cast(situacao as string) as ds_status,
        
        --Audit
        cast(dataHoraGeracao as datetime) as dt_record_creation,
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed