# Sales data

```sql pedidos
select * from bigquery_gold.pedidos
```

```sql pedidos_por_data
select 
  month(data_emissao) as "mês",
  count(distinct codPedido) as "pedidos"
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

