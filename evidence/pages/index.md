# 📊 Dashboard de Vendas (Caché Legacy)

<Alert status="info">
  Dados processados via dbt e extraídos automaticamente da VPS.
</Alert>

```sql vendas_diarias
select 
    data_emissao, 
    sum(valorTotalOriginal) as faturamento,
    count(ID) as total_pedidos
from bronze.stg_pedidos
group by 1
order by 1