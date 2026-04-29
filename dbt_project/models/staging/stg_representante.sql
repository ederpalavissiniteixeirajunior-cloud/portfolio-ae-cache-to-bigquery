with source as (
    select * from {{ source('raw_cache', 'representantes') }}
),

renamed as (
    select
        
        cast(ID as string) as ID,
        {{ mask_pii_info('CNPJCPF') }} as CNPJCPF,
        cast(cidade as string) as cidade,
        cast(codEmpresa as int64) as codEmpresa,
        cast(codIBGE as string) as codIBGE,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as codRepresentante,
        cast(codSituacao as string) as codSituacao,
        cast(codTipoComissao as string) as codTipoComissao,
        cast(dataHoraGeracao as datetime) as dataHoraGeracao,
        {{ mask_pii_info('emailRepresentante','email') }} as emailRepresentante,
        cast(estado as string) as estado,
        cast(geoRepresentante as string) as geoRepresentante,
        cast(idRepresentante as string) as idRepresentante,
        {{ mask_pii_info('nome','name') }} as nome,
        cast(pais as string) as pais,
        cast(percComissao as float64) as percComissao,
        cast(percComissaoFaturamento as float64) as percComissaoFaturamento,
        cast(percComissaoLiquidacao as float64) as percComissaoLiquidacao,
        cast(regiao as string) as regiao,
        cast(situacao as string) as situacao,
        cast(tipoComissao as string) as tipoComissao,
        cast(extracted_at as timestamp) as extracted_at,
        {{ generate_audit_columns() }}
        
        from source
)

select * from renamed
