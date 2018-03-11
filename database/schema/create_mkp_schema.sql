--
-- Create the database
CREATE DATABASE IF NOT EXISTS mkp_products ;

--
-- switch to the newly created db
use mkp_products ;

--
-- create the order source table
CREATE TABLE IF NOT EXISTS order_sources
(
    source_name     VARCHAR(50)  NOT NULL                                                       -- Channel where order was taken
   ,latest_user     VARCHAR(30)      NULL                                                       -- Latest user to update row
   ,latest_update   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)      NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(source_name)
) ;

DESCRIBE order_channels ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER order_channel_create_trigger BEFORE INSERT on order_channels
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER order_channel_update_trigger BEFORE UPDATE on order_channels
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- create the expense table
CREATE TABLE IF NOT EXISTS expenses
(
    id                   INT UNSIGNED  NOT NULL AUTO_INCREMENT                                        -- Unique ID for the record
   ,source_name          VARCHAR(50)   NOT NULL                                                       -- Channel where order was taken
   ,expense_datetime     TIMESTAMP     NOT NULL                                                       -- "date/time"
   ,type                 VARCHAR(50)       NULL                                                       -- "date/time"
   ,description          VARCHAR(150)      NULL                                                       -- "date/time"
   ,total                DECIMAL(13,2) NOT NULL                                                       -- amount of the expense
   ,latest_user          VARCHAR(30)       NULL                                                       -- Latest user to update row
   ,latest_update        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user        VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,INDEX (type)
   ,INDEX (expense_datetime)
   ,FOREIGN KEY (source_name) REFERENCES order_sources (source_name)
   ,PRIMARY KEY(id)
) ;

DESCRIBE expenses ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER expense_create_trigger BEFORE INSERT on expenses
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER expense_update_trigger BEFORE UPDATE on expenses
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

-- vendor domain data
CREATE TABLE if not exists vendors
(
    vendor_name   VARCHAR(50)  NOT NULL                                                       -- vendor name
   ,description   VARCHAR(150)     NULL                                                       -- description
   ,latest_user   VARCHAR(30)      NULL                                                       -- Latest user to update row
   ,latest_update TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user VARCHAR(30)      NULL                                                       -- User that created the row
   ,creation_date TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY (vendor_name)
) ;

DESCRIBE vendors ;
--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER vendor_create_trigger BEFORE INSERT on vendors
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER vendor_update_trigger BEFORE UPDATE on vendors
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- SKU Domain data
CREATE TABLE if not exists skus
(
    sku             VARCHAR(20)   NOT NULL                                                       -- our internal sku id
   ,vendor_name     VARCHAR(50)       NULL                                                       -- name of the vendor we buy the sku from
   ,title           VARCHAR(150)      NULL                                                       -- title of the listing
   ,description     VARCHAR(500)      NULL                                                       -- details of the SKU
   ,latest_user     VARCHAR(30)       NULL                                                       -- Latest user to update row                                                       --
   ,latest_update   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,FOREIGN KEY (vendor_name) REFERENCES vendors(vendor_name)
   ,PRIMARY KEY (sku)
) ;

DESCRIBE skus ;
--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER sku_create_trigger BEFORE INSERT on skus
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER sku_update_trigger BEFORE UPDATE on skus
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- SKU Costs Domain data
CREATE TABLE if not exists sku_costs
(
    sku             VARCHAR(20)   NOT NULL                                                       -- our internal sku id
   ,cost            DECIMAL(13,2) NOT NULL                                                       -- current price
   ,start_date      DATE          NOT NULL                                                       -- the starting date when the cost is valid
   ,end_date        DATE              NULL                                                       -- the last date the cost is valid
   ,latest_user     VARCHAR(30)       NULL                                                       -- Latest user to update row                                                       --
   ,latest_update   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,FOREIGN KEY (sku) REFERENCES skus (sku)
   ,PRIMARY KEY (sku,start_date)
) ;

DESCRIBE sku_costs ;

--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER sku_cost_create_trigger BEFORE INSERT on sku_costs
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER sku_cost_update_trigger BEFORE UPDATE on sku_costs
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- SKU Costs Domain data
CREATE TABLE if not exists inventory_conditions
(
    condition_name VARCHAR(30)   NOT NULL                                                       -- Inventory Condition
   ,description    VARCHAR(30)   NOT NULL                                                       -- Inventory Condition
   ,latest_user    VARCHAR(30)       NULL                                                       -- Latest user to update row                                                       --
   ,latest_update  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user  VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY (condition_name)
) ;

desc inventory_conditions ;

--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER inventory_condition_create_trigger BEFORE INSERT on inventory_conditions
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER inventory_condition_update_trigger BEFORE UPDATE on inventory_conditions
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- inventory reports
CREATE TABLE if not exists onhand_inventory_reports
(
    id              INT UNSIGNED  NOT NULL AUTO_INCREMENT                                        -- primary key
   ,sku             VARCHAR(20)   NOT NULL                                                       -- our internal sku id
   ,report_date     DATE          NOT NULL                                                       -- date report was run
   ,source_name     VARCHAR(50)   NOT NULL                                                       -- Channel where inventroy is
   ,condition_name  VARCHAR(30)   NOT NULL                                                       -- Current condition of inventory
   ,quantity        INT UNSIGNED  NOT NULL                                                       -- Number of units
   ,latest_user     VARCHAR(30)       NULL                                                       -- Latest user to update row                                                       --
   ,latest_update   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,FOREIGN KEY (sku)            REFERENCES skus (sku)
   ,FOREIGN KEY (source_name)    REFERENCES order_sources (source_name)
   ,FOREIGN KEY (condition_name) REFERENCES inventory_conditions (condition_name)
   ,PRIMARY KEY (id)
) ;

desc onhand_inventory_reports ;

--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER onhand_inventory_report_create_trigger BEFORE INSERT on onhand_inventory_reports
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER onhand_inventory_report_update_trigger BEFORE UPDATE on onhand_inventory_reports
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;
--
-- SKU Domain data
CREATE TABLE if not exists active_sources
(
    sku             VARCHAR(20)   NOT NULL                                                       -- our internal sku id
   ,sku_source_id   VARCHAR(50)       NULL                                                       -- unique identifier for this SKU on this source
   ,source_name     VARCHAR(50)       NULL                                                       -- the website in question
   ,active          BOOLEAN       NOT NULL                                                       -- is the sku active at this source
   ,latest_user     VARCHAR(30)       NULL                                                       -- Latest user to update row                                                       --
   ,latest_update   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,FOREIGN KEY (source_name) REFERENCES order_sources (source_name)
   ,PRIMARY KEY (sku)
) ;

DESCRIBE active_sources ;
--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER active_source_create_trigger BEFORE INSERT on active_sources
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER active_source_update_trigger BEFORE UPDATE on active_sources
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- create the order source table
CREATE TABLE IF NOT EXISTS financial_event_groups
(
    id                           INT UNSIGNED  NOT NULL AUTO_INCREMENT                                        -- default primary key
   ,source_name                  VARCHAR(50)   NOT NULL                                                       -- Channel where order was taken
   ,ext_financial_event_group_id VARCHAR(50)   NOT NULL                                                       -- source name's event group id
   ,fund_transfer_dt             TIMESTAMP         NULL                                                       -- start time of the financial event group
   ,transfer_status              VARCHAR(50)       NULL                                                       -- status of the financial transfer
   ,processing_status            VARCHAR(50)   NOT NULL                                                       -- status of the process transfer
   ,event_start_dt               TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- start time of the financial event group
   ,event_end_dt                 TIMESTAMP         NULL                                                       -- end time of the financial event group
   ,trace_id                     VARCHAR(50)       NULL                                                       -- source name's trace id
   ,account_tail                 VARCHAR(50)       NULL                                                       -- last 4 digits of the account of the transfer
   ,beginning_balance            DECIMAL(13,2) NOT NULL                                                       -- balance
   ,total                        DECIMAL(13,2) NOT NULL                                                       -- current balance
   ,currency_code                VARCHAR(3)    NOT NULL                                                       -- currency code used for all currencies
   ,latest_user                  VARCHAR(30)       NULL                                                       -- Latest user to update row
   ,latest_update                TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user                VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date                TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(id)
   ,UNIQUE (ext_financial_event_group_id)
) ;

DESCRIBE financial_event_groups ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER feg_create_trigger BEFORE INSERT on financial_event_groups
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER feg_update_trigger BEFORE UPDATE on financial_event_groups
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- create the table for orders
create table if not exists financial_shipment_events
(
    id                           INT UNSIGNED     NOT NULL AUTO_INCREMENT                                       -- Unique ID for the record
   ,feg_id                       INT UNSIGNED     NOT NULL                                                      -- financial event group (parent)
   ,event_type                   VARCHAR(50)      NOT NULL                                                      -- Where the order was from
   ,posted_dt                    TIMESTAMP            NULL                                                      -- "date/time"
   ,source_order_id              VARCHAR(50)      NOT NULL                                                      -- "marketplace"
   ,marketplace                  VARCHAR(50)      NOT NULL                                                      -- "marketplace"
   ,sku                          VARCHAR(20)      NOT NULL                                                      -- "sku"
   ,quantity                     INT UNSIGNED     NOT NULL                                                      -- "quantity"
   ,product_charges              DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,product_charges_tax          DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,shipping_charges             DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,shipping_charges_tax         DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,giftwrap_charges             DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,giftwrap_charges_tax         DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,marketplace_facilitator_tax  DECIMAL(13,2)    NOT NULL                                                      -- where amazon returns the taxes
   ,promotional_rebates          DECIMAL(13,2)    NOT NULL                                                      -- "selling fees"
   ,selling_fees                 DECIMAL(13,2)    NOT NULL                                                      -- "selling fees"
   ,fba_fees                     DECIMAL(13,2)    NOT NULL                                                      -- "fba fees"
   ,other_fees                   DECIMAL(13,2)    NOT NULL                                                      -- "other"
   ,total                        DECIMAL(13,2)    NOT NULL                                                      -- "total"
   ,currency_code                VARCHAR(3)    NOT NULL                                                       -- currency code used for all currencies
   ,latest_user                  VARCHAR(30)         NULL                                                       -- Latest user to update row
   ,latest_update                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user                VARCHAR(30)         NULL                                                       -- User that created the row
   ,creation_date                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,INDEX (posted_dt)
   ,INDEX (sku)
   ,INDEX (source_order_id)
   ,UNIQUE (posted_dt,event_type,source_order_id,sku)
   ,FOREIGN KEY (feg_id)      REFERENCES financial_event_groups (id)
   ,FOREIGN KEY (sku)         REFERENCES skus (sku)
   ,PRIMARY KEY (id)
) ;

DESCRIBE financial_shipment_events ;

--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER fse_create_trigger BEFORE INSERT on financial_shipment_events
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER fse_update_trigger BEFORE UPDATE on financial_shipment_events
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

--
-- create the expense table
CREATE TABLE IF NOT EXISTS financial_expense_events
(
    id                   INT UNSIGNED  NOT NULL AUTO_INCREMENT                                        -- Unique ID for the record
   ,feg_id               INT UNSIGNED  NOT NULL                                                      -- financial event group (parent)
   ,expense_dt           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- "date/time"
   ,type                 VARCHAR(50)       NULL                                                       -- "date/time"
   ,description          VARCHAR(150)      NULL                                                       -- "date/time"
   ,total                DECIMAL(13,2) NOT NULL                                                       -- amount of the expense
   ,currency_code        VARCHAR(3)    NOT NULL                                                       -- currency code used for all currencies
   ,latest_user          VARCHAR(30)       NULL                                                       -- Latest user to update row
   ,latest_update        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user        VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,INDEX (type)
   ,INDEX (expense_dt)
   ,UNIQUE (feg_id,expense_dt,type)
   ,FOREIGN KEY (feg_id)      REFERENCES financial_event_groups (id)
   ,PRIMARY KEY(id)
) ;

DESCRIBE financial_expense_events ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER fee_create_trigger BEFORE INSERT on expenses
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER fee_update_trigger BEFORE UPDATE on expenses
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

CREATE TABLE IF NOT EXISTS order_channel_credentials
(
    source_name     VARCHAR(50)  NOT NULL                                                             -- Channel where order was taken
   ,credentials     VARCHAR(250) NOT NULL                                                             -- key value pairs
   ,latest_user          VARCHAR(30)       NULL                                                       -- Latest user to update row
   ,latest_update        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user        VARCHAR(30)       NULL                                                       -- User that created the row
   ,creation_date        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(source_name)
   ,FOREIGN KEY (source_name) REFERENCES order_sources (source_name)
) ;

DESCRIBE order_channel_credentials ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER occ_create_trigger BEFORE INSERT on order_channel_credentials
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER occ_update_trigger BEFORE UPDATE on order_channel_credentials
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

CREATE TABLE IF NOT EXISTS inbound_shipments
(
    id                   INT UNSIGNED NOT NULL AUTO_INCREMENT                                        -- Unique ID for the record
   ,source_name          VARCHAR(50)  NOT NULL                                                       -- Channel where order was taken
   ,condition_name       VARCHAR(50)  NOT NULL                                                       -- shipment status
   ,ext_shipment_id      VARCHAR(50)  NOT NULL                                                       -- Source Name's id for the shipment
   ,ext_shipment_name    VARCHAR(50)  NOT NULL                                                       -- Source Name's name for the shipment
   ,destination          VARCHAR(50)  NOT NULL                                                       -- Warehouse destination
   ,latest_user          VARCHAR(30)      NULL                                                       -- Latest user to update row
   ,latest_update        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user        VARCHAR(30)      NULL                                                       -- User that created the row
   ,creation_date        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(id)
   ,FOREIGN KEY (source_name) REFERENCES order_sources (source_name)
   ,INDEX(ext_shipment_id)
   ,UNIQUE(ext_shipment_id)
) ;

DESCRIBE inbound_shipments ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER inbound_shipment_create_trigger BEFORE INSERT on inbound_shipments
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER inbound_shipment_update_trigger BEFORE UPDATE on inbound_shipments
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

CREATE TABLE IF NOT EXISTS inbound_shipment_items
(
    id                   INT UNSIGNED NOT NULL AUTO_INCREMENT                                        -- Unique ID for the record
   ,sku                  VARCHAR(20)  NOT NULL                                                       -- Our internal sku id
   ,inbound_shipment_id  INT UNSIGNED NOT NULL                                                       -- Parent id
   ,quantity_shipped     INT UNSIGNED NOT NULL                                                       -- Quantity shipped to destination
   ,quantity_in_case     INT UNSIGNED NOT NULL                                                       -- Quantity of sku per case
   ,quantity_received    INT UNSIGNED NOT NULL                                                       -- Quantity received by destination
   ,latest_user          VARCHAR(30)      NULL                                                       -- Latest user to update row
   ,latest_update        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user        VARCHAR(30)      NULL                                                       -- User that created the row
   ,creation_date        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(id)
   ,FOREIGN KEY (inbound_shipment_id) REFERENCES inbound_shipments (id)
   ,FOREIGN KEY (sku) REFERENCES skus (sku)
   ,UNIQUE(id,sku)
   ,INDEX(id)
   ,INDEX(sku)
) ;

DESCRIBE inbound_shipment_items ;

--
-- Create trigger to get the user who created or udpated
DELIMITER //
CREATE TRIGGER isi_create_trigger BEFORE INSERT on inbound_shipment_items
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER isi_update_trigger BEFORE UPDATE on inbound_shipment_items
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;

