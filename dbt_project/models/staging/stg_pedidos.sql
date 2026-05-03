with source as (
    select * from {{ source('raw_cache', 'pedidos') }}
),

renamed as (
    select
        
        cast(ID as string) as ID,
        safe_cast(safe_cast(codAtendente as float64) as int64) as codAtendente,
        safe_cast(safe_cast(codCliente as float64) as int64) as codCliente,
        safe_cast(safe_cast(codEmpresa as float64) as int64) as codEmpresa,
        safe_cast(safe_cast(codEstatisticaFaturamento as float64) as int64) as codEstatisticaFaturamento,
        safe_cast(safe_cast(codEstatisticaVendas as float64) as int64) as codEstatisticaVendas,
        safe_cast(safe_cast(codLote as float64) as int64) as codLote,
        safe_cast(safe_cast(codNota as float64) as int64) as codNota,
        safe_cast(safe_cast(codOperador as float64) as int64) as codOperador,
        safe_cast(safe_cast(codPedido as float64) as int64) as codPedido,
        safe_cast(safe_cast(codPedidoAutomacao as float64) as int64) as codPedidoAutomacao,
        safe_cast(safe_cast(codPedidoCliente as float64) as int64) as codPedidoCliente,
        safe_cast(safe_cast(codPedidoRepresentante as float64) as int64) as codPedidoRepresentante,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as codRepresentante,
        safe_cast(safe_cast(codSituacao as float64) as int64) as codSituacao,
        safe_cast(safe_cast(codSituacaoDuplicata as float64) as int64) as codSituacaoDuplicata,
        safe_cast(safe_cast(codTipoCobranca as float64) as int64) as codTipoCobranca,
        safe_cast(safe_cast(codTipoDesconto as float64) as int64) as codTipoDesconto,
        safe_cast(safe_cast(codTipoPedido as float64) as int64) as codTipoPedido,
        safe_cast(safe_cast(codTipoPedidoMalhConf as float64) as int64) as codTipoPedidoMalhConf,
        cast(codTransportadora as string) as codTransportadora,
        cast(condVenda as string) as condVenda,
        
        safe_cast(nullif(dataCancelamento, 'NaT') as date) as data_cancelamento,
        safe_cast(nullif(dataDigitacao, 'NaT') as date) as data_digitacao,
        safe_cast(nullif(dataEmissao, 'NaT') as date) as data_emissao,
        safe_cast(nullif(dataEmissaoNF, 'NaT') as date) as data_emissao_nf,
        safe_cast(nullif(dataHoraGeracao, 'NaT') as datetime) as data_hora_geracao,
        safe_cast(nullif(dataHoraLiberacaoComercial, 'NaT') as date) as data_hora_liberacao_comercial,
        safe_cast(nullif(dataHoraLiberacaoFinanceiro, 'NaT') as datetime) as data_hora_liberacao_financeiro,
        safe_cast(nullif(dataPrevisaoFat, 'NaT') as date) as data_previsao_fat,

        
        cast(valorTotalOriginal as float64) as valorTotalOriginal,
        cast(valorCancelado as float64) as valorCancelado,
        cast(valorFaturado as float64) as valorFaturado,

        safe_cast(safe_cast(qtdItens as float64) as int64) as qtdItens,
        safe_cast(safe_cast(qtdItensOriginal as float64) as int64) as qtdItensOriginal,
        
        cast(situacao as string) as situacao,
        cast(tabelaPreco as string) as tabelaPreco,
        cast(nomeOperador as string) as nomeOperador,
        
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed
