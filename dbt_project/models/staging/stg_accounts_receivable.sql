-- models/staging/stg_accounts_receivable.sql

with source as (
    select * from {{ source('raw_cache', 'contas_receber') }}
),

renamed as (
    select
        -- Keys
        cast(ID as string) as nk_accounts_receivable,
        cast(codEmpresa as int64) as cd_company,
        cast(idEmpresa as string) as id_company_uuid,
        cast(codCliente as int64) as cd_customer,
        cast(idCliente as string) as id_customer_uuid,
        cast(codRepresentante as int64) as cd_representative,
        cast(idRepresentante as string) as id_representative_uuid,
        cast(codDuplicata as string) as cd_invoice_number,
        cast(idDuplicata as string) as id_invoice_uuid,

        -- PII Masked Info
        {{ mask_pii_info('agenciaBancaria') }} as cd_bank_branch,
        {{ mask_pii_info('atendente') }} as ds_attendant_name,
        {{ mask_pii_info('nomeCliente','name') }} as ds_customer_name,
        {{ mask_pii_info('nomeRepresentante','name') }} as ds_representative_name,
        {{ mask_pii_info('razaoEmpresa','name') }} as ds_company_name,

        -- Dates & Timestamps
        cast(dataEmissao as date) as dt_issue,
        cast(dataVencOriginal as date) as dt_original_due,
        cast(dataVencimento as date) as dt_due,
        cast(dataPagamento as date) as dt_payment,
        cast(dataBxContabil as date) as dt_accounting_write_off,
        cast(dataHoraGeracao as datetime) as dt_record_creation,
        cast(dataFluxoCaixa as string) as ds_cash_flow_period,

        -- Financial Amounts (Values)
        {{ convert_money('vlrOriginal') }} as vl_original_amount,
        {{ convert_money('vlrTitulo') }} as vl_receivable_amount,
        {{ convert_money('vlrContratual') }} as vl_contractual_amount,
        {{ convert_money('vlrTotalNF') }} as vl_total_invoice_amount,
        {{ convert_money('vlrDesconto') }} as vl_discount_amount,
        {{ convert_money('vlrJuros') }} as vl_interest_amount,
        {{ convert_money('vlrMultaAtraso') }} as vl_late_fee_amount,
        {{ convert_money('vlrMultaPaga') }} as vl_paid_fine_amount,
        {{ convert_money('vlrCorrMonet') }} as vl_monetary_correction_amount,

        -- Tax Retentions
        {{ convert_money('vlrRetCOFINS') }} as vl_tax_retention_cofins,
        {{ convert_money('vlrRetCSSL') }} as vl_tax_retention_cssl,
        {{ convert_money('vlrRetIR') }} as vl_tax_retention_ir,
        {{ convert_money('vlrRetPIS') }} as vl_tax_retention_pis,

        -- Commission Details
        cast(baseComissFat as float64) as vl_commission_base_invoiced,
        cast(baseComissLiq as float64) as vl_commission_base_paid,
        cast(comissFat as float64) as vl_commission_amount_invoiced,
        cast(comissLiq as float64) as vl_commission_amount_paid,
        {{ convert_money('vlrComisLiqRep') }} as vl_rep_commission_paid,
        {{ convert_money('vlrComLiqRep') }} as vl_rep_comm_net,

        -- Attendant Commissions
        cast(baseComissAtenFat as float64) as vl_attendant_commission_base_invoiced,
        cast(baseComissAtenLiq as float64) as vl_attendant_commission_base_paid,
        cast(comissAtenFat as float64) as vl_attendant_commission_amount_invoiced,
        cast(comissAtenLiq as float64) as vl_attendant_commission_amount_paid,
        {{ convert_money('vlrComisLiqAte') }} as vl_attendant_commission_paid,
        {{ convert_money('vlrComLiqAte') }} as vl_attendant_comm_net,

        -- Percentages
        cast(percComissao as float64) as pc_rep_commission,
        cast(percComissaoAten as float64) as pc_attendant_commission,
        cast(percDesconto as float64) as pc_discount,
        cast(prazoConcedido as int64) as nr_days_term_granted,
        cast(prazoRealizado as int64) as nr_days_term_actual,
        cast(descPrazoConcedido as string) as ds_term_granted_desc,
        cast(descPrazoRealizado as string) as ds_term_actual_desc,
        cast(diasDescDupl as string) as nr_invoice_discount_days,

        -- Qualitative Info
        cast(situacao as string) as ds_status,
        cast(tipoDocumento as string) as ds_document_type,
        cast(tipodeCobranca as string) as ds_billing_type,
        cast(tipoBaixa as string) as ds_write_off_type,
        cast(tipoDesconto as string) as ds_discount_type,
        cast(condicaoVenda as string) as ds_sales_condition,
        cast(moeda as string) as cd_currency,
        cast(portador as string) as ds_bearer_bank,
        cast(numBancario as string) as cd_bank_document_number,
        cast(listada as string) as fl_is_listed,
        cast(priNotaFiscal as int64) as nr_first_invoice,
        cast(qtdeNotaFiscal as int64) as qt_invoices,
        cast(qtdIndexador as float64) as qt_index_units,

        -- Audit
        cast(extracted_at as datetime) as extracted_at,
        {{ generate_audit_columns() }}

    from source
)

select * from renamed