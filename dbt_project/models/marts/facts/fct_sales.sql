with orders as (
    select * from {{ ref('stg_pedidos') }}
),

--order_items as (
 --   select * from {{ ref('stg_vendas_itens') }}
--),

final as (
    select
        *

    from orders
)

select * from final