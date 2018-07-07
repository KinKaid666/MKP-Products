select 'Orders without costs' ;
select a.source_order_id
       , a.sku
       , a.posted_dt
       , a.cost
  from (
        select so.source_order_id
               , so.sku
               , so.posted_dt
               , sc.cost
          from financial_shipment_events so
          left outer join sku_costs sc
            on so.sku = sc.sku
           and sc.start_date < so.posted_dt
           and (sc.end_date is null or
                sc.end_date > so.posted_dt)
       ) a
 where a.cost is null
;

select 'SKUs with orders without costs' ;
select distinct a.sku
  from (
        select so.source_order_id
               , so.sku
               , so.posted_dt
               , sc.cost
          from financial_shipment_events so
          left outer join sku_costs sc
            on so.sku = sc.sku
           and sc.start_date < so.posted_dt
           and (sc.end_date is null or
                sc.end_date > so.posted_dt)
       ) a
 where a.cost is null
 order by a.sku
;

select 'Currenty inventory without costs' ;
select distinct a.sku
       ,a.quantity_total
  from (
        select ri.sku
               ,ri.quantity_total
               ,sc.cost
          from realtime_inventory ri
          left outer join sku_costs sc
            on sc.sku = ri.sku
       ) a
 where a.cost is null
   and a.quantity_total > 0
 order by a.sku
;

select 'SKUs with Unknown details' ;
select sku
  from skus
 where vendor_name = 'Unknown'
   or description = 'Unknown'
;
