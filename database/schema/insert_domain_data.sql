--
-- Select our DB incase it's not selected already
use mkp_products ;

--
-- insert order_channels
INSERT INTO order_channels ( source_name ) VALUES
    ( 'www.amazon.com' ),
    ( 'www.ebay.com' ) ;

--
-- Insert vendors
INSERT INTO vendors ( vendor_name, description ) VALUES
    ( 'Wooster'            ,'' ),
    ( 'Adfors FIbaFuse'    ,'' ),
    ( 'HANDy'              ,'' ),
    ( 'Hyde Tools'         ,'' ),
    ( 'MaxxGrip'           ,'' ),
    ( 'Paint Scentsations' ,'' ),
    ( 'Tower Sealants'     ,'' ),
    ( 'Warner'             ,'' ),
    ( 'Whizz'              ,'' ) ;

--
-- insert inventory_conditions
INSERT INTO inventory_conditions ( condition_name, description ) VALUES
    ('SELLABLE', 'Available to sell'),
    ('UNSELLABLE', 'Not currently for sale') ;
