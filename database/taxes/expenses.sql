select a.month,
       sum(a.amount) amount,
       a.expense_type
  from (
        select month(e.expense_datetime) month,
               sum(e.total) amount,
               e.type expense_type
          from expenses e
         where year(e.expense_datetime) = @year
           -- and e.type in ('Salary', 'Rent'); per Kyle include Comissions and tech
         group by 1, 3
        union all
        select month(fee.expense_dt) month,
               sum(fee.total) amount,
               fee.type
          from financial_expense_events fee
         where year(fee.expense_dt) = @year
         group by 1,3
  ) a
  where a.amount != 0
  group by 1, 3
  order by 1, 2

;
