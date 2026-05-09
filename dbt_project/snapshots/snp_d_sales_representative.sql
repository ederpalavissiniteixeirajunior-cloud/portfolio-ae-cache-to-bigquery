{% snapshot snp_d_sales_representative %}

{{
    config(
      target_schema='silver',
      unique_key='sk_sales_representative',
      strategy='check',
      check_cols=[ 
        'cd_sales_representative',
        'nk_sales_representatives',
        'ds_email', 
        'ds_representative_name',
        'ds_city', 
        'ds_state',
        'cd_status'
        ],
      invalidate_hard_deletes=True
    )
}}


select * from {{ ref('itm_d_sales_representative') }}

{% endsnapshot %}