# Sales data

```sql pedidos
select * from bigquery_silver.itm_f_orders
```

```sql pedidos_por_data
select 
  month(dt_issued) as "mês",
  count(distinct cd_order) as "pedidos"
  from bigquery_silver.itm_f_orders
  group by "mês"
```

<LineChart
  data={pedidos_por_data}
  x=mês
  y=pedidos
/>


<BigValue
  data={pedidos}
  value=valorTotalOriginal
/>

