select year(expense_datetime)
       ,month(expense_datetime)
       ,sum(total) total
       ,type
       ,description
  from expenses
 where year(expense_datetime) = @year
   and month(expense_datetime) = @month
 group by year(expense_datetime)
          ,month(expense_datetime)
          ,type
          ,description
 order by year(expense_datetime)
          ,month(expense_datetime)
          ,type
          ,description
;
