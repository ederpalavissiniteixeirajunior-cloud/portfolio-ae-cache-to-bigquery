with source as (
    select * from {{ source('raw_cache', 'clientes') }}
),

renamed as (
    select
        cast(ID as string) as nk_customer,
        cast(codEmpresa as int64) as cd_company,
        {{ mask_pii_info('CEP') }} as cd_cep,
        {{ mask_pii_info('CNPJCPF') }} as cd_cpf_cnpj,
        {{ mask_pii_info('GrupoCNPJ') }} as cd_group_cnpj,
        {{ mask_pii_info('celularDadosComplementares') }} as nr_cellphone,
        cast(cidade as string) as ds_city,
        cast(clienteDesde as date) as dt_start_customer,
        safe_cast(safe_cast(codCliente as float64) as int64) as cd_customer,
        cast(codIBGE as string) as cd_ibge,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as cd_representative,
        cast(codRepresentante2 as string) as cd_representative2,
        cast(codRepresentante3 as string) as cd_representative3,
        cast(codRepresentante4 as string) as cd_representative4,
        cast(codRepresentante5 as string) as cd_representative5,
        cast(codRepresentante6 as string) as cd_representative6,
        cast(codSituacao as string) as cd_situation,
        cast(dataHoraGeracao as datetime) as dt_creation,
        {{ mask_pii_info('descricaoGrupoCNPJ') }} as ds_group_cnpj,
        {{ mask_pii_info('email', 'email') }} as ds_email,
        {{ mask_pii_info('endereco') }} as ds_adress,
        cast(estado as string) as ds_state,
        cast(geoCliente as string) as geoCliente,
        cast(idCliente as string) as id_customer,
        cast(inscEstadual as string) as cd_state_inscrition,
        {{ mask_pii_info('nomeFantasia') }} as ds_trade_name,
        cast(pais as string) as ds_country,
        cast(ramoAtividade as string) as ds_line_business,
        {{ mask_pii_info('razao', 'name') }} as ds_corporate_name,
        {{ mask_pii_info('razaoEmpresa', 'name') }} as ds_company_name,
        cast(regiao as string) as ds_region,
        cast(situacao as string) as ds_status,
        {{ mask_pii_info('telefone') }} as nr_telephone,
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed
