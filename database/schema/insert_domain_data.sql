--
-- Select our DB incase it's not selected already
use mkp_products ;

--
-- insert order_channels
INSERT INTO order_sources ( source_name ) VALUES
    ( 'www.amazon.com' ),
    ( 'www.ebay.com' ) ;

--
-- Insert vendors
INSERT INTO vendors ( vendor_name, description ) VALUES
    ( 'Wooster'            ,'' ),
    ( 'Adfors'             ,'' ),
    ( 'HANDy'              ,'' ),
    ( 'Convenience'        ,'' ),
    ( 'Hyde Tools'         ,'' ),
    ( 'MaxxGrip'           ,'' ),
    ( 'Paint Scentsations' ,'' ),
    ( 'Tower Sealants'     ,'' ),
    ( 'Warner'             ,'' ),
    ( 'Whizz'              ,'' ),
    ( 'Foremost'           ,'' ),
    ( 'Unknown'            ,'' ) ;

--
-- insert inventory_conditions
INSERT INTO inventory_conditions ( condition_name, description ) VALUES
    ('SELLABLE', 'Available to sell'),
    ('UNSELLABLE', 'Not currently for sale') ;

