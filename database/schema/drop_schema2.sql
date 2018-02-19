use mkp_products2 ;

DROP TRIGGER IF EXISTS active_source_update_trigger ;
DROP TRIGGER IF EXISTS active_source_create_trigger ;
DROP TABLE IF EXISTS active_sources ;

DROP TRIGGER IF EXISTS onhand_inventory_report_update_trigger ;
DROP TRIGGER IF EXISTS onhand_inventory_report_create_trigger ;
DROP TABLE IF EXISTS onhand_inventory_reports ;

DROP TRIGGER IF EXISTS inventory_condition_update_trigger ;
DROP TRIGGER IF EXISTS inventory_condition_create_trigger ;
DROP TABLE IF EXISTS inventory_conditions ;

DROP TRIGGER IF EXISTS sku_cost_update_trigger ;
DROP TRIGGER IF EXISTS sku_cost_create_trigger ;
DROP TABLE IF EXISTS sku_costs ;

DROP TRIGGER IF EXISTS fee_update_trigger ;
DROP TRIGGER IF EXISTS fee_create_trigger ;
DROP TABLE IF EXISTS financial_expense_events ;

DROP TRIGGER IF EXISTS fse_update_trigger ;
DROP TRIGGER IF EXISTS fse_create_trigger ;
DROP TABLE IF EXISTS financial_shipment_events ;

DROP TRIGGER IF EXISTS feg_update_trigger ;
DROP TRIGGER IF EXISTS feg_create_trigger ;
DROP TABLE IF EXISTS financial_event_groups ;

DROP TRIGGER IF EXISTS sku_update_trigger ;
DROP TRIGGER IF EXISTS sku_create_trigger ;
DROP TABLE IF EXISTS skus ;

DROP TRIGGER IF EXISTS vendor_update_trigger ;
DROP TRIGGER IF EXISTS vendor_create_trigger ;
DROP TABLE IF EXISTS vendors ;

DROP TRIGGER IF EXISTS order_sources_update_trigger ;
DROP TRIGGER IF EXISTS order_sources_create_trigger ;
DROP TABLE IF EXISTS order_sources ;


DROP DATABASE IF EXISTS mkp_products2 ;

