select 'Orders without costs' ;
select a.source_order_id
       , a.sku
       , a.order_datetime
       , a.cost
  from (
        select so.source_order_id
               , so.sku
               , so.order_datetime
               , sc.cost
          from sku_orders so
          left outer join sku_costs sc
            on so.sku = sc.sku
           and sc.start_date < so.order_datetime
           and (sc.end_date is null or
                sc.end_date > so.order_datetime)
       ) a
 where a.cost is null
;

select 'SKUs with orders without costs' ;
select distinct a.sku
  from (
        select so.source_order_id
               , so.sku
               , so.order_datetime
               , sc.cost
          from sku_orders so
          left outer join sku_costs sc
            on so.sku = sc.sku
           and sc.start_date < so.order_datetime
           and (sc.end_date is null or
                sc.end_date > so.order_datetime)
       ) a
 where a.cost is null
 order by a.sku
;

select 'Currenty inventory without costs' ;
select distinct a.sku
       ,a.condition_name
       ,a.quantity
  from (
        select ohi.sku
               ,ohi.report_date
               ,ohi.source_name
               ,ohi.condition_name
               ,ohi.quantity
               ,sc.cost
          from onhand_inventory_reports ohi
          left outer join sku_costs sc
            on sc.sku = ohi.sku
         where report_date = ( select max(report_date) from onhand_inventory_reports )
       ) a
 where a.cost is null
   and a.quantity > 0
 order by a.sku
;

select 'SKUs with Unknown details' ;
select sku
  from skus
 where vendor_name = 'Unknown'
   or description = 'Unknown'
;
