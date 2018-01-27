--
-- Create admin user to create and setup database
CREATE USER mkp_admin@localhost IDENTIFIED BY 'admin_temp' ;
GRANT ALL PRIVILEGES ON *.* TO mkp_admin@localhost ;

--
-- create read-only accounts
CREATE USER ericferg_ro@'%' IDENTIFIED BY 'ericferg_ro_2018' ;
CREATE USER markprey_ro@'%' IDENTIFIED BY 'markprey_ro_2018' ;

GRANT SELECT ON mkp_products.* TO ericferg_ro@'%' ;
GRANT SELECT ON mkp_products.* TO markprey_ro@'%' ;

--
-- Create a user who can create reports
CREATE USER mkp_reporter@localhost IDENTIFIED BY 'mkp_reporter_2018' ; GRANT SELECT ON mkp_products.* TO mkp_reporter@localhost ;

--
-- Create a user who can load orders
CREATE USER mkp_loader@localhost IDENTIFIED BY 'mkp_loader_2018' ;
GRANT INSERT,SELECT,UPDATE,DELETE ON mkp_products.* TO mkp_loader@localhost ;

--
-- Create user session manager
CREATE USER usertable@localhost IDENTIFIED BY '2018userLogin' ;
GRANT INSERT,SELECT,UPDATE,DELETE ON usertable.* TO usertable@localhost ;

--
-- reload all the privileges
FLUSH PRIVILEGES ;
