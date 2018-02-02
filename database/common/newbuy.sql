    select min(order_datetime) oldest_order
           ,so.sku
           ,ifnull(last_onhand_inventory_report.source_name, "N/A") source_name
           ,ifnull(last_onhand_inventory_report.condition_name, "N/A") condition_name
           ,ifnull(last_onhand_inventory_report.quantity, 0) quantity
           ,ifnull(last_onhand_inventory_report.quantity, 0)*sc.cost value
           ,count(distinct so.source_order_id      ) order_count
           ,sum(case when so.type = 'Refund' then -1 * CAST(so.quantity as SIGNED) else 1 * CAST(so.quantity as SIGNED) end) unit_count
           ,sum(case when so.type = 'Refund' then -1 * CAST(so.quantity as SIGNED) else 1 * CAST(so.quantity as SIGNED) end) / (180/7)  weekly_velocity
           ,ifnull(last_onhand_inventory_report.quantity, 0) / (sum(case when so.type = 'Refund' then -1 * CAST(so.quantity as SIGNED) else 1 * CAST(so.quantity as SIGNED) end) / (180/7)) woc
           ,sum(so.product_sales                   ) product_sales
           ,sum(shipping_credits                   ) +
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
                 sum(case when so.type = 'Refund' then sc.cost*so.quantity*1 else sc.cost*so.quantity*-1 end) contrib_margin
      from sku_orders so
      join sku_costs sc
        on so.sku = sc.sku
       and sc.start_date < so.order_datetime
       and (sc.end_date is null or
            sc.end_date > so.order_datetime)
      left outer join (
            select ohi.sku
                   ,ohi.report_date
                   ,ohi.source_name
                   ,ohi.condition_name
                   ,ohi.quantity
              from onhand_inventory_reports ohi
             where report_date = ( select max(report_date) from onhand_inventory_reports )
          ) last_onhand_inventory_report
        on last_onhand_inventory_report.sku = so.sku
     where so.order_datetime > NOW() - INTERVAL 180 DAY
     group by sku
              ,sc.cost
              ,last_onhand_inventory_report.source_name
              ,last_onhand_inventory_report.condition_name
              ,last_onhand_inventory_report.quantity
     order by contrib_margin
;
