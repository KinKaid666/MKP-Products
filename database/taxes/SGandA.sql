select month(e.expense_datetime) month,
       sum(e.total) amount,
       e.type expense_type
  from expenses e
 where year(e.expense_datetime) = @year
   and e.type in ('Salary', 'Rent')
 group by 1, 3
 order by 1, 3
;
