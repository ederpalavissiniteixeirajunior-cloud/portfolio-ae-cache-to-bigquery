with source as (
    select * from {{ source('raw_cache', 'clientes') }}
),

renamed as (
    select
        cast(ID as string) as ID,
        {{ mask_pii_info('CEP') }} as CEP,
        {{ mask_pii_info('CNPJCPF') }} as CNPJCPF,
        {{ mask_pii_info('GrupoCNPJ') }} as GrupoCNPJ,
        {{ mask_pii_info('celularDadosComplementares') }} as celularDadosComplementares,
        cast(cidade as string) as cidade,
        cast(clienteDesde as date) as clienteDesde,
        safe_cast(safe_cast(codCliente as float64) as int64) as codCliente,
        cast(codIBGE as string) as codIBGE,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as codRepresentante,
        cast(codRepresentante2 as string) as codRepresentante2,
        cast(codRepresentante3 as string) as codRepresentante3,
        cast(codRepresentante4 as string) as codRepresentante4,
        cast(codRepresentante5 as string) as codRepresentante5,
        cast(codRepresentante6 as string) as codRepresentante6,
        cast(codSituacao as string) as codSituacao,
        cast(dataHoraGeracao as datetime) as dataHoraGeracao,
        {{ mask_pii_info('descricaoGrupoCNPJ') }} as descricaoGrupoCNPJ,
        {{ mask_pii_info('email', 'email') }} as email,
        {{ mask_pii_info('endereco') }} as endereco,
        cast(estado as string) as estado,
        cast(geoCliente as string) as geoCliente,
        cast(idCliente as string) as idCliente,
        cast(inscEstadual as string) as inscEstadual,
        {{ mask_pii_info('nomeFantasia') }} as nomeFantasia,
        cast(pais as string) as pais,
        cast(ramoAtividade as string) as ramoAtividade,
        {{ mask_pii_info('razao', 'name') }} as razao,
        {{ mask_pii_info('razaoEmpresa', 'name') }} as razaoEmpresa,
        cast(regiao as string) as regiao,
        cast(situacao as string) as situacao,
        {{ mask_pii_info('telefone') }} as telefone,
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed
