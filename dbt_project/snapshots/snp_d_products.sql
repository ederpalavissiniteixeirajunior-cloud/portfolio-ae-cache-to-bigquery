{% snapshot snp_d_products %}

{{
    config(
      target_schema='silver',
      unique_key='sk_product',
      strategy='check',
      check_cols=[  'ds_product', 'ds_product_reference','ds_color'],
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('itm_d_products') }}

{% endsnapshot %}