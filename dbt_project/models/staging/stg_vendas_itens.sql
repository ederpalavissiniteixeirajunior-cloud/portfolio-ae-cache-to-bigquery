with source as (
    select * from {{ source('raw_cache', 'vendas_itens') }}
),

renamed as (
    select
        
        cast(ID as string) as ID,
        cast(codColecao as string) as codColecao,
        cast(codEmpresa as int64) as codEmpresa,
        cast(codGrade as string) as codGrade,
        cast(codPedido as int64) as codPedido,
        cast(codProduto as int64) as codProduto,
        cast(codSituacaoItem as int64) as codSituacaoItem,
        cast(codTipoDesconto as int64) as codTipoDesconto,
        cast(dataHoraGeracao as datetime) as dataHoraGeracao,
        cast(dataPrevisaoFatItemPed as string) as dataPrevisaoFatItemPed,
        cast(descColecao as string) as descColecao,
        cast(descGrade as string) as descGrade,
        cast(descProdutoGen as string) as descProdutoGen,
        cast(descProdutoSaida as string) as descProdutoSaida,
        cast(desconto as float64) as desconto,
        cast(idEmpresa as string) as idEmpresa,
        cast(idPedido as string) as idPedido,
        cast(idPedidoItem as string) as idPedidoItem,
        cast(idProduto as string) as idProduto,
        cast(loteFabricacao as string) as loteFabricacao,
        cast(mascara as string) as mascara,
        cast(percentualComissao as string) as percentualComissao,
        safe_cast(safe_cast(qtdAtendida as float64) as int64) as qtdAtendida,
        safe_cast(safe_cast(qtdBloqueada as float64) as int64) as qtdBloqueada,
        safe_cast(safe_cast(qtdCancelada as float64) as int64) as qtdCancelada,
        safe_cast(safe_cast(qtdPedida as float64) as int64) as qtdPedida,
        cast(razaoEmpresa as string) as razaoEmpresa,
        cast(seqItem as int64) as seqItem,
        cast(situacaoItem as string) as situacaoItem,
        cast(tabelaPreco as string) as tabelaPreco,
        cast(tipoDesconto as string) as tipoDesconto,
        cast(unidadeMedida as string) as unidadeMedida,
        {{ convert_money('valorCancelado') }} as valorCancelado,
        {{ convert_money('valorFaturado') }} as valorFaturado,
        {{ convert_money('valorOriginal') }} as valorOriginal,
        {{ convert_money('valorSaldo') }} as valorSaldo,
        {{ convert_money('valorTotalME') }} as valorTotalME,
        {{ convert_money('valorUnitario') }} as valorUnitario,
        {{ convert_money('valorUnitarioLiquido') }} as valorUnitarioLiquido,
        {{ convert_money('valorUnitarioME') }} as valorUnitarioME,
        cast(extracted_at as timestamp) as extracted_at,
        {{ generate_audit_columns() }}
        
        from source
)

select * from renamed
