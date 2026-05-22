with source as (
    select * from {{ source('raw_cache', 'vendas_itens') }}
),

renamed as (
    select
        -- Keys & Identifiers
        cast(id as string) as nk_order_item,
        cast(idpedidoitem as string) as id_order_item_uuid,
        cast(idpedido as string) as id_order_uuid,
        cast(idproduto as string) as id_product_uuid,
        cast(idempresa as string) as id_company_uuid,
        
        -- Business Codes
        safe_cast(codpedido as int64) as cd_order,
        safe_cast(codproduto as int64) as cd_product,
        safe_cast(codempresa as int64) as cd_company,
        cast(codcolecao as string) as cd_collection,
        cast(codgrade as string) as cd_grid,
        cast(codsituacaoitem as string) as cd_item_status,
        cast(codtipodesconto as string) as cd_discount_type,
        cast(lotefabricacao as string) as cd_manufacturing_batch,
        
        -- Descriptions & Attributes
        cast(desccolecao as string) as ds_collection,
        cast(descgrade as string) as ds_grid,
        cast(descprodutogen as string) as ds_product_generic_desc,
        cast(descprodutosaida as string) as ds_product_output_desc,
        cast(situacaoitem as string) as ds_item_status_desc,
        cast(tipodesconto as string) as ds_discount_type_desc,
        cast(tabelapreco as string) as ds_price_table,
        cast(unidademedida as string) as ds_unit_of_measure,
        cast(mascara as string) as ds_mask,
        cast(razaoempresa as string) as ds_company_corporate_name,
        cast(seqitem as string) as nr_item_sequence,
        
        -- Dates & Timestamps
        cast(datahorageracao as datetime) as dt_generated_at,
        cast(dataprevisaofatitemped as string) as ds_estimated_billing_date,
        
        -- Quantities
        safe_cast(safe_cast(qtdatendida as float64) as int64) as qt_fulfilled,
        safe_cast(safe_cast(qtdbloqueada as float64) as int64) as qt_blocked,
        safe_cast(safe_cast(qtdcancelada as float64) as int64) as qt_canceled,
        safe_cast(safe_cast(qtdpedida as float64) as int64) as qt_ordered,
        
        -- Percentages
        cast(percentualcomissao as float64) as pc_commission,
        cast(desconto as float64) as pc_discount,
        
        -- Monetary Values
        {{ convert_money('valorcancelado') }} as vl_canceled_amount,
        {{ convert_money('valorfaturado') }} as vl_billed_amount,
        {{ convert_money('valororiginal') }} as vl_original_amount,
        {{ convert_money('valorsaldo') }} as vl_balance_amount,
        {{ convert_money('valortotalme') }} as vl_total_me,
        {{ convert_money('valorunitario') }} as vl_unit_price,
        {{ convert_money('valorunitarioliquido') }} as vl_net_unit_price,
        {{ convert_money('valorunitariome') }} as vl_unit_price_me,
        
        -- Audit
        cast(extracted_at as timestamp) as extracted_at,
        {{ generate_audit_columns() }}
        
    from source
)

select * from renamed