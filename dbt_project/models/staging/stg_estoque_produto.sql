with source as (
    select * from {{ source('raw_cache', 'estoque_produto') }}
),

renamed as (
    select
        cast(ID as string) as ID,
        cast(codEmpresa as int64) as codEmpresa,
        cast(codNatureza as int64) as codNatureza,
        cast(codProduto as int64) as codProduto,
        cast(dataHoraGeracao as datetime) as dataHoraGeracao,
        cast(idEstoqueProduto as string) as idEstoqueProduto,
        cast(qtdeEstReservaLotes as float64) as qtdeEstReservaLotes,
        cast(qtdeEstReservaPedido as float64) as qtdeEstReservaPedido,
        cast(qtdeEstReservaProducao as float64) as qtdeEstReservaProducao,
        cast(qtdeEstoqueAtual as float64) as qtdeEstoqueAtual,
        cast(qtdeEstoqueMaximo as float64) as qtdeEstoqueMaximo,
        cast(qtdeEstoqueMinimo as float64) as qtdeEstoqueMinimo,
        {{ mask_pii_info('razaoEmpresa','name') }} as razaoEmpresa,
        {{ convert_money('vlrPrecoMedio') }} as vlrPrecoMedio,
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed
