-- Standardizing and cleaning customer data before history tracking
with staging as (
    select * from {{ ref('stg_customers') }}
),

intermediate as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['cd_customer  ']) }} as sk_customer  ,
        cd_customer,
        nk_customer,
        cd_representative,
        upper(ds_corporate_name) as ds_corporate_name,
        cd_cpf_cnpj,
        lower(ds_email) as ds_email,
        nr_cellphone,
        ds_adress,
        cd_cep,
        ds_city,
        ds_state,
        ds_country,
        dt_start_customer  ,
        current_timestamp() as updated_at
    from staging
    where cd_company = 1
)

select * from intermediate