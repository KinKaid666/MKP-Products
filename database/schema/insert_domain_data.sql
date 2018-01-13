--
-- Select our DB incase it's not selected already
use mkp_products ;

--
-- insert order_channels
INSERT INTO order_channels ( source ) VALUES
  ( 'Amazon' ),
  ( 'eBay' ) ;

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
