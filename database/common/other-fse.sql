select event_type
       ,posted_dt
       ,source_order_id
       ,marketplace
       ,sku
       ,quantity
       -- ,product_charges
       -- ,product_charges_tax
       -- ,shipping_charges
       -- ,shipping_charges_tax
       -- ,giftwrap_charges
       -- ,giftwrap_charges_tax
       -- ,marketplace_facilitator_tax
       -- ,promotional_rebates
       -- ,selling_fees
       -- ,fba_fees
       ,other_fees
       ,total
       ,currency_code
  from financial_shipment_events fse
 where event_type not in ('Order','Refund')
;
