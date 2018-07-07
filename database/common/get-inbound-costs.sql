select fee.type
       , sum(fee.total) total
  from financial_expense_events fee
 where fee.type like '%Inbound%'
   -- and fee.expense_dt > MAKEDATE(2018,1)
 group by fee.type
;

select sum(isi.quantity_shipped) shipped
       , sum(isi.quantity_received) received
  from inbound_shipments s
  join inbound_shipment_items isi
    on isi.inbound_shipment_id = s.id
 -- group by s.id
;
