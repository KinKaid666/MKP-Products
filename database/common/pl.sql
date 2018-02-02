select sku_activity_by_month.year
       ,sku_activity_by_month.month
       , order_count
       , unit_count
       , product_sales
       , (shipping_credits + gift_wrap_credits + promotional_rebates + sales_tax_collected + marketplace_facilitator_tax + transaction_fees + other + selling_fees + fba_fees) selling_fees
       , cogs
       , ifnull(expenses_by_month.expenses,0) expenses
       , ifnull(sga_by_month.expenses,0) sga
       , (product_sales + shipping_credits + gift_wrap_credits + promotional_rebates + sales_tax_collected + marketplace_facilitator_tax + transaction_fees + other + selling_fees + fba_fees + cogs + ifnull(expenses_by_month.expenses,0) + ifnull(sga_by_month.expenses,0) ) net_income
  from ( select date_format(so.order_datetime,"%Y") year
                ,date_format(so.order_datetime, "%m") month
                ,count(distinct so.source_order_id      ) order_count
                ,sum(so.quantity                        ) unit_count
                ,sum(so.product_sales                   ) product_sales
                ,sum(shipping_credits                   ) shipping_credits
                ,sum(gift_wrap_credits                  ) gift_wrap_credits
                ,sum(promotional_rebates                ) promotional_rebates
                ,sum(sales_tax_collected                ) sales_tax_collected
                ,sum(marketplace_facilitator_tax        ) marketplace_facilitator_tax
                ,sum(transaction_fees                   ) transaction_fees
                ,sum(other                              ) other
                ,sum(so.selling_fees                    ) selling_fees
                ,sum(so.fba_fees                        ) fba_fees
                ,sum(case when so.type = 'Refund' then sc.cost*so.quantity*1 else sc.cost*so.quantity*-1 end) cogs
           from sku_orders so
           join sku_costs sc
             on so.sku = sc.sku
            and sc.start_date < so.order_datetime
            and (sc.end_date is null or
                 sc.end_date > so.order_datetime)
         group by date_format(so.order_datetime,"%Y")
                  ,date_format(so.order_datetime,"%m")
  ) as sku_activity_by_month
  left outer join ( select date_format(e.expense_datetime,"%Y") year
                ,date_format(e.expense_datetime,"%m") month
                ,sum(e.total) expenses
           from expenses e
          where type <> "Salary"
            and type <> "Rent"
         group by date_format(e.expense_datetime,"%Y")
                  ,date_format(e.expense_datetime,"%m")
       ) expenses_by_month
    on sku_activity_by_month.year = expenses_by_month.year
   and sku_activity_by_month.month = expenses_by_month.month
  left outer join ( select date_format(e.expense_datetime,"%Y") year
                ,date_format(e.expense_datetime,"%m") month
                ,sum(e.total) expenses
           from expenses e
          where type = "Salary"
             or type = "Rent"
         group by date_format(e.expense_datetime,"%Y")
                  ,date_format(e.expense_datetime,"%m")
       ) sga_by_month
    on sku_activity_by_month.year = sga_by_month.year
   and sku_activity_by_month.month = sga_by_month.month
 order by year, month
;
