select expense_datetime
       , description
       , total
   from expenses
  where type = 'Salary'
  order by expense_datetime
;
