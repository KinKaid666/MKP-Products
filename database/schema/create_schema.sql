--
-- Create the database
CREATE DATABASE IF NOT EXISTS mkp_products ;

--
-- switch to the newly created db
use mkp_products ;

--
-- create the order channel domain tabe
CREATE TABLE IF NOT EXISTS order_channels
(
    id              INT UNSIGNED NOT NULL         AUTO_INCREMENT                                -- Unique ID for the record
   ,source          VARCHAR(20)  NOT NULL                                                       -- Channel where order was taken
   ,latest_user     VARCHAR(15)      NULL                                                       -- Latest user to update row
   ,latest_update   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user   VARCHAR(30)      NULL                                                       -- User that created the row
   ,creation_date   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,PRIMARY KEY(id)
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
-- create the table for orders
create table if not exists sku_orders
(
    id                           INT UNSIGNED     NOT NULL AUTO_INCREMENT                                       -- Unique ID for the record
   ,source_id                    INT UNSIGNED     NOT NULL                                                      -- Channel where order was taken
   ,order_datetime               TIMESTAMP        NOT NULL                                                      -- "date/time"
   ,settlement_id                BIGINT UNSIGNED  NOT NULL                                                      -- "settlement id"
   ,type                         VARCHAR(100)     NOT NULL                                                      -- "type""
   ,source_order_id              VARCHAR(100)         NULL                                                      -- "order id"
   ,sku                          VARCHAR(20)      NOT NULL                                                      -- "sku"
   ,quantity                     INT UNSIGNED     NOT NULL                                                      -- "quantity"
   ,marketplace                  VARCHAR(50)      NOT NULL                                                      -- "marketplace"
   ,fulfillment                  VARCHAR(50)      NOT NULL                                                      -- "fulfillment"
   ,order_city                   VARCHAR(50)      NOT NULL                                                      -- "order city"
   ,order_state                  VARCHAR(50)      NOT NULL                                                      -- "order state"
   ,order_postal_code            VARCHAR(12)      NOT NULL                                                      -- "order postal"
   ,product_sales                DECIMAL(13,2)    NOT NULL                                                      -- "product sales"
   ,shipping_credits             DECIMAL(13,2)    NOT NULL                                                      -- "shipping credits"
   ,gift_wrap_credits            DECIMAL(13,2)    NOT NULL                                                      -- "gift wrap credits"
   ,promotional_rebates          DECIMAL(13,2)    NOT NULL                                                      -- "promotional rebates"
   ,sales_tax_collected          DECIMAL(13,2)    NOT NULL                                                      -- "sales tax collected"
   ,marketplace_facilitator_tax  DECIMAL(13,2)    NOT NULL                                                      -- "Marketplace Facilitator Tax"
   ,selling_fees                 DECIMAL(13,2)    NOT NULL                                                      -- "selling fees"
   ,fba_fees                     DECIMAL(13,2)    NOT NULL                                                      -- "fba fees"
   ,transaction_fees             DECIMAL(13,2)    NOT NULL                                                      -- "other transaction fees"
   ,other                        DECIMAL(13,2)    NOT NULL                                                      -- "other"
   ,total                        DECIMAL(13,2)    NOT NULL                                                      -- "total"
   ,latest_user                  VARCHAR(30)         NULL                                                       -- Latest user to update row
   ,latest_update                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Latest time row updated
   ,creation_user                VARCHAR(30)         NULL                                                       -- User that created the row
   ,creation_date                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP                             -- Time row created
   ,FOREIGN KEY (source_id) REFERENCES order_channels (id)
   ,PRIMARY KEY (id)
) ;

DESCRIBE sku_orders ;

--
-- Create trigger to get the use who created or udpated
DELIMITER //
CREATE TRIGGER order_create_trigger BEFORE INSERT on sku_orders
FOR EACH ROW
BEGIN
    set NEW.creation_user = USER() ;
    set NEW.latest_user = USER() ;
END //
CREATE TRIGGER order_update_trigger BEFORE UPDATE on sku_orders
FOR EACH ROW
BEGIN
    set NEW.latest_user = USER() ;
END //
DELIMITER ;
