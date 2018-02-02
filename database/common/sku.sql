select s.sku
       ,s.description
       ,v.vendor_name
       ,v.description
  from skus s
  join vendors v
    on s.vendor_name = v.vendor_name
 where s.sku = @sku
;

select sc.sku
       ,sc.cost
       ,sc.start_date
       ,sc.end_date
  from sku_costs sc
 where sc.sku = @sku
;

select date_format(so.order_datetime,"%Y") year
       ,date_format(so.order_datetime, "%m") month
       ,so.sku
       ,count(distinct so.source_order_id      ) order_count
       ,sum(so.quantity                        ) unit_count
       ,sum(so.product_sales                   ) product_sales
       , sum(shipping_credits                   ) +
             sum(gift_wrap_credits                  ) +
             sum(promotional_rebates                ) +
             sum(sales_tax_collected                ) +
             sum(marketplace_facilitator_tax        ) +
             sum(transaction_fees                   ) +
             sum(other                              ) +
             sum(so.selling_fees                    ) selling_fees
       ,sum(so.fba_fees                        ) fba_fees
       ,sum(case when so.type = 'Refund' then sc.cost*so.quantity*1 else sc.cost*so.quantity*-1 end) cogs
       ,sum(so.product_sales                   ) +
             sum(shipping_credits                   ) +
             sum(gift_wrap_credits                  ) +
             sum(promotional_rebates                ) +
             sum(sales_tax_collected                ) +
             sum(marketplace_facilitator_tax        ) +
             sum(transaction_fees                   ) +
             sum(other                              ) +
             sum(so.selling_fees                    ) +
             sum(so.fba_fees                        ) +
             sum(case when so.type = 'Refund' then sc.cost*so.quantity*1 else sc.cost*so.quantity*-1 end) net_income
  from sku_orders so
  join sku_costs sc
    on so.sku = sc.sku
   and sc.start_date < so.order_datetime
   and (sc.end_date is null or
        sc.end_date > so.order_datetime)
 where so.sku = @sku
group by date_format(so.order_datetime,"%Y")
         ,date_format(so.order_datetime,"%m")
         ,sku
order by date_format(so.order_datetime,"%Y")
         ,date_format(so.order_datetime,"%m")
         ,sku
;

select so.order_datetime
       ,so.sku
       ,so.source_order_id
       ,so.type
       ,so.quantity
       ,so.product_sales
       ,so.shipping_credits
       ,so.gift_wrap_credits
       ,so.promotional_rebates
       ,so.sales_tax_collected
       ,so.marketplace_facilitator_tax
       ,so.transaction_fees
       ,so.other
       ,so.selling_fees
       ,so.fba_fees
  from sku_orders so
 where so.sku = @sku
 order by so.order_datetime
;
