with source as (
    select * from {{ source('raw_cache', 'tabela_preco_item') }}
),

renamed as (
    select
        cast(ID as string) as ID,
        cast(codEmpresa as int64) as codEmpresa,
        cast(codFaixa as string) as codFaixa,
        cast(codOrigemPreco as string) as codOrigemPreco,
        cast(codProduto as int64) as codProduto,
        cast(codTabela as int64) as codTabela,
        cast(condicaoVenda as string) as condicaoVenda,
        cast(dataHoraGeracao as datetime) as dataHoraGeracao,
        cast(idTabelaPreco as string) as idTabelaPreco,
        cast(idTabelaPrecoItem as string) as idTabelaPrecoItem,
        cast(observacao as string) as observacao,
        cast(origemPreco as string) as origemPreco,
        cast(percAcrescimo as float64) as percAcrescimo,
        cast(percentualMaxDesconto as float64) as percentualMaxDesconto,
        {{ convert_money('precoTabela') }} as precoTabela,
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed