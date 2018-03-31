use mkp_products ;

DROP TRIGGER IF EXISTS active_source_update_trigger ;
DROP TRIGGER IF EXISTS active_source_create_trigger ;
DROP TABLE IF EXISTS active_sources ;

DROP TRIGGER IF EXISTS realtime_inventory_update_trigger ;
DROP TRIGGER IF EXISTS realtime_inventory_create_trigger ;
DROP TABLE IF EXISTS realtime_inventory ;

DROP TRIGGER IF EXISTS sku_cost_update_trigger ;
DROP TRIGGER IF EXISTS sku_cost_create_trigger ;
DROP TABLE IF EXISTS sku_costs ;

DROP TRIGGER IF EXISTS sku_case_packs_update_trigger ;
DROP TRIGGER IF EXISTS sku_case_packs_create_trigger ;
DROP TABLE IF EXISTS sku_case_packs ;

DROP TRIGGER IF EXISTS sku_update_trigger ;
DROP TRIGGER IF EXISTS sku_create_trigger ;
DROP TABLE IF EXISTS skus ;

DROP TRIGGER IF EXISTS vendor_update_trigger ;
DROP TRIGGER IF EXISTS vendor_create_trigger ;
DROP TABLE IF EXISTS vendors ;

DROP TRIGGER IF EXISTS expsense_update_trigger ;
DROP TRIGGER IF EXISTS expsense_create_trigger ;
DROP TABLE IF EXISTS expsenses ;

DROP TRIGGER IF EXISTS order_channel_update_trigger ;
DROP TRIGGER IF EXISTS order_channel_create_trigger ;
DROP TABLE IF EXISTS order_channels ;

DROP TRIGGER IF EXISTS fee_update_trigger ;
DROP TRIGGER IF EXISTS fee_create_trigger ;
DROP TABLE IF EXISTS financial_expense_events ;

DROP TRIGGER IF EXISTS fse_update_trigger ;
DROP TRIGGER IF EXISTS fse_create_trigger ;
DROP TABLE IF EXISTS financial_shipment_events ;

DROP TRIGGER IF EXISTS feg_update_trigger ;
DROP TRIGGER IF EXISTS feg_create_trigger ;
DROP TABLE IF EXISTS financial_event_groups ;

DROP TRIGGER IF EXISTS occ_update_trigger ;
DROP TRIGGER IF EXISTS occ_create_trigger ;
DROP TABLE IF EXISTS order_channel_credentials ;

DROP TRIGGER IF EXISTS isi_update_trigger ;
DROP TRIGGER IF EXISTS isi_create_trigger ;
DROP TABLE IF EXISTS inbound_shipment_items ;

DROP TRIGGER IF EXISTS inbound_shipment_update_trigger ;
DROP TRIGGER IF EXISTS inbound_shipment_create_trigger ;
DROP TABLE IF EXISTS inbound_shipments ;

DROP DATABASE IF EXISTS mkp_products ;

use usertable

DROP TABLE IF EXISTS users ;
DROP TABLE IF EXISTS user_cookies ;

DROP DATABASE IF EXISTS usertable ;
