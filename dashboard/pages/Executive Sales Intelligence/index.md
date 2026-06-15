---
title: Executive Sales Intelligence
sidebar_position: 1
---

*Selected Analysis Scope: **{inputs.collection_selector}** Collection*

<Alert status="warning">
 This dashboard simulates a localized retail operation based in Brazil. All currency metrics are natively rendered in Brazilian Real (BRL / R$) to reflect the exact financial schemas ingested from the legacy database.
</Alert>


```sql options
select
    collection_name
from
    analytics.sales_vs_target_cumulative
group by collection_name
order by min(id_collection)
```

```sql latest_collection
select collection_name
from analytics.sales_vs_target_cumulative
group by collection_name
order by max(reference_date) desc
limit 1
```

<ButtonGroup 
    data={options} 
    name=collection_selector 
    value=collection_name
    display=tabs
    defaultValue={latest_collection[0].collection_name}
/>

```sql executive_cards
select
    *
from
    analytics.vw_executive_kpis
where 
    collection_name = '${inputs.collection_selector}'
```


```sql cumulative
select
    reference_date,
    cumulative_sales,
    cumulative_target
from
    analytics.sales_vs_target_cumulative
where 
    collection_name = '${inputs.collection_selector}'
```
  <BigValue data={executive_cards} value=total_sales title="Total Sales" fmt=brl2m />
  <BigValue data={executive_cards} value=total_target title="Total Target Sales" fmt=brl2m />
  <BigValue data={executive_cards} value=target_attainment title="Attainment" fmt=pct />
  <BigValue data={executive_cards} value=total_customers title="Active Customers" fmt=id />


<LineChart 
    data={cumulative}
    x=reference_date 
    y={['cumulative_sales', 'cumulative_target']}
    title="Cumulative - Sales vs. Target"
    fmt=brl2m
    yfmt=brl
    tooltipFmt={['brl', 'brl']}
    xAxisTitle="Date"
    yAxisTitle="Revenue (BRL)"
    colorPalette={['#5eff00', '#ff0000']}
    fillOpacity=0.1
/>

```sql top_reps
select
    *
from
    analytics.vw_reps_performance_base
where 
    collection_name = '${inputs.collection_selector}'
order by total_sales desc limit 5
```

<BarChart 
    data={top_reps}
    x=representative 
    y=total_sales
    swapXY=true
    yFmt=brl0k
    labels=true
    labelPosition=inside
    title="🏆 Top 5 Sales Representatives"
    fillColor={['#1eff00']}
/>

```sql top_products
select
    *
from
    analytics.vw_top_products_base
where 
    collection_name = '${inputs.collection_selector}'
order by total_quantity desc limit 5
```

<BarChart 
    data={top_products}
    x=product
    y=total_quantity
    swapXY=true
    labels=true
    labelPosition=inside
    title="Top 5 products sales"    
    fillColor={['#5eff00']}
/>

```sql top_products_amount
select
    *
from
    analytics.vw_top_products_base
where 
    collection_name = '${inputs.collection_selector}'
order by total_revenue desc limit 5
```

<BarChart 
    data={top_products_amount}
    x=product
    y=total_revenue
    swapXY=true
    labels=true
    yFmt=brl2k
    labelPosition=inside
    title="Top 5 products revenue"    
    fillColor={['#5eff00']}
/>

<LastRefreshed/>