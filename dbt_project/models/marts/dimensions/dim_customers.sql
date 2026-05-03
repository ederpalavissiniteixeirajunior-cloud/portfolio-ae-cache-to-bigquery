with source as (
    select * from {{ ref('snp_d_customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sk_customer', 'dbt_updated_at']) }} as sk_customer_version,
        sk_customer, 
        cd_customer,
        nk_customer,
        ds_corporate_name,
        cd_cpf_cnpj,
        ds_email,
        nr_cellphone,
        ds_adress,
        cd_cep,
        ds_city,
        ds_state,
        ds_country,
        dt_start_customer,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current
    from source
)

select * from final