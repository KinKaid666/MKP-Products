select ohi.sku
       ,ohi.report_date
       ,ohi.source_name
       ,ohi.condition_name
       ,ohi.quantity
  from onhand_inventory_reports ohi
 where report_date = ( select max(report_date) from onhand_inventory_reports )
;
