with source as (
    select * from {{ source('raw_cache', 'pedidos') }}
),

renamed as (
    select
        -- 1. Identifiers (id)
        cast(ID as string) as nk_order, 
        
        -- 2. Codes
        safe_cast(safe_cast(codPedido as float64) as int64) as cd_order,
        safe_cast(safe_cast(codCliente as float64) as int64) as cd_customer,
        cast(codEmpresa as int64) as cd_company,
        safe_cast(safe_cast(codAtendente as float64) as int64) as cd_attendant,
        safe_cast(safe_cast(codOperador as float64) as int64) as cd_operator,
        safe_cast(safe_cast(codRepresentante as float64) as int64) as cd_sales_representative,
        cast(codTransportadora as string) as cd_carrier,
        safe_cast(safe_cast(codSituacao as float64) as int64) as cd_status,
        safe_cast(safe_cast(codTipoPedido as float64) as int64) as cd_order_type,
        safe_cast(safe_cast(codTipoCobranca as float64) as int64) as cd_billing_type,
        safe_cast(safe_cast(codTipoDesconto as float64) as int64) as cd_discount_type,
        safe_cast(safe_cast(codNota as float64) as int64) as cd_invoice,
        safe_cast(safe_cast(codLote as float64) as int64) as cd_batch,
        safe_cast(safe_cast(codPedidoAutomacao as float64) as int64) as cd_automation_order,
        safe_cast(safe_cast(codPedidoCliente as float64) as int64) as cd_customer_order,
        safe_cast(safe_cast(codPedidoRepresentante as float64) as int64) as cd_representative_order,
        safe_cast(safe_cast(codEstatisticaFaturamento as float64) as int64) as cd_billing_stats,
        safe_cast(safe_cast(codEstatisticaVendas as float64) as int64) as cd_sales_stats,
        safe_cast(safe_cast(codSituacaoDuplicata as float64) as int64) as cd_duplicate_status,
        safe_cast(safe_cast(codTipoPedidoMalhConf as float64) as int64) as cd_knitting_order_type,
        cast(condVenda as string) as cd_sale_condition,
        cast(tabelaPreco as string) as cd_price_table,

        -- 3. Names / Descriptions (nm)
        cast(situacao as string) as nm_status,
        cast(nomeOperador as string) as nm_operator,

        -- 4. Dates / Timestamps (dt)
        safe_cast(nullif(dataEmissao, 'NaT') as date) as dt_issued,
        safe_cast(nullif(dataEmissaoNF, 'NaT') as date) as dt_invoice_issued,
        safe_cast(nullif(dataDigitacao, 'NaT') as date) as dt_input,
        safe_cast(nullif(dataPrevisaoFat, 'NaT') as date) as dt_estimated_billing,
        safe_cast(nullif(dataCancelamento, 'NaT') as date) as dt_cancelled,
        safe_cast(nullif(dataHoraGeracao, 'NaT') as datetime) as dt_generated,
        safe_cast(nullif(dataHoraLiberacaoComercial, 'NaT') as datetime) as dt_commercial_released,
        safe_cast(nullif(dataHoraLiberacaoFinanceiro, 'NaT') as datetime) as dt_financial_released,

        -- 5. Values / Amounts (vl)
        cast(valorTotalOriginal as float64) as vl_original_total,
        cast(valorCancelado as float64) as vl_cancelled,
        cast(valorFaturado as float64) as vl_billed,

        -- 6. Quantities (qt)
        -- Seguindo a lógica de abreviação do projeto para quantidades
        safe_cast(safe_cast(qtdItens as float64) as int64) as qt_items,
        safe_cast(safe_cast(qtdItensOriginal as float64) as int64) as qt_original_items,
        
        -- Audit & Metadata
        cast(extracted_at as datetime) as dt_extracted,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed