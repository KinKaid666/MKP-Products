select a.posted_dt posted
       , sum(sales) sales
        ,sum(sales) OVER (ORDER BY a.posted_dt ASC ROWS 6 PRECEDING ) AS '7-day sum'
        ,avg(sales) OVER (ORDER BY a.posted_dt ASC ROWS 6 PRECEDING ) AS '7-day avg'
        ,sum(sales) OVER (ORDER BY a.posted_dt ASC ROWS 29 PRECEDING) AS '30-day sum'
        ,avg(sales) OVER (ORDER BY a.posted_dt ASC ROWS 29 PRECEDING) AS '30-day avg'
  from (
      select date_format(posted_dt, "%Y-%m-%d") posted_dt
             , sum(product_charges + shipping_charges + giftwrap_charges + product_charges_tax + shipping_charges_tax + giftwrap_charges_tax) sales
        from financial_shipment_events fse
       where posted_dt >= DATE(NOW() - INTERVAL 30 DAY)
       group by date_format(posted_dt, "%Y-%m-%d")
) a
group by a.posted_dt
order by a.posted_dt desc ;

select row_number() over (order by sales desc) id, date_format(posted_dt, "%Y-%m-%d") posted_dt
       , sum(product_charges + shipping_charges + giftwrap_charges + product_charges_tax + shipping_charges_tax + giftwrap_charges_tax) sales
  from financial_shipment_events fse
 group by date_format(posted_dt, "%Y-%m-%d")
 order by 3 desc
limit 50 ;
