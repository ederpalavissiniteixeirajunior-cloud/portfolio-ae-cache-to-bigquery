{% snapshot snp_d_customer %}

{{
    config(
      target_schema='silver',
      unique_key='cd_customer',
      strategy='check',
      check_cols=[ 
        'cd_representative',
        'ds_corporate_name',
        'cd_cpf_cnpj',
        'ds_email', 
        'nr_cellphone',
        'ds_adress',
        'cd_cep',
        'ds_city', 
        'ds_state'
        ],
      invalidate_hard_deletes=True
    )
}}


select * from {{ ref('itm_d_customer') }}

{% endsnapshot %}