# Sales data

```sql pedidos
select * from bigquery_gold.pedidos
```

```sql pedidos_por_data
select 
  month(dt_issued) as "mês",
  count(distinct cd_order) as "pedidos"
  from bigquery_gold.pedidos
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

