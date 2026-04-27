with source as (
    select * from {{ source('raw_cache', 'clientes') }}
),

renamed as (
    select
        cast(ID as string) as ID,
        cast(CEP as string) as CEP,
        cast(CNPJCPF as string) as CNPJCPF,
        cast(GrupoCNPJ as string) as GrupoCNPJ,
        cast(celularDadosComplementares as string) as celularDadosComplementares,
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
        cast(descricaoGrupoCNPJ as string) as descricaoGrupoCNPJ,
        cast(email as string) as demail,
        cast(endereco as string) as endereco,
        cast(estado as string) as estado,
        cast(geoCliente as string) as geoCliente,
        cast(idCliente as string) as idCliente,
        cast(inscEstadual as string) as inscEstadual,
        cast(nomeFantasia as string) as nomeFantasia,
        cast(pais as string) as pais,
        cast(ramoAtividade as string) as ramoAtividade,
        cast(razao as string) as razao,
        cast(razaoEmpresa as string) as razaoEmpresa,
        cast(regiao as string) as regiao,
        cast(situacao as string) as situacao,
        cast(telefone as string) as telefone,
        cast(extracted_at as datetime) as extracted_at

    from source
)

select * from renamed
