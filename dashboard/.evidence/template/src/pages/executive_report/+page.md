---
title: Executive Sales Intelligence
sidebar_position: 1
---

```sql options
select distinct
        collection_name
from
    analytics.sales_vs_target_cumulative
ORDER BY id_collection
```



<ButtonGroup 
    data={options} 
    name=collection_selector 
    value=collection_name
    display=tabs
    defaultValue="SPRING27"
/>

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
<BigValue 
  data={cumulative} 
  value=Total
  --comparison=1
  title='Total Amount'
  fmt=brl
  comparisonFmt=pct1
  comparisonTitle="vs. Last Month"
/>

<LineChart 
    data={cumulative}
    x=reference_date 
    y={['cumulative_sales', 'cumulative_target']}
    title="Cumulative Sales vs. cumulative target"
    fmt=brl2m
    yfmt=brl
    tooltipFmt={['brl', 'brl']}
    xAxisTitle="Date"
    yAxisTitle="Revenue (BRL)"
    colorPalette={['#003cff', '#ff0000']}
    fillOpacity=0.1
/>

<LastRefreshed/>