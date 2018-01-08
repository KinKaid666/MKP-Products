--
-- Drop mkp users
DROP USER mkp_admin@localhost ;
DROP USER ericferg_ro@'%' ;
DROP USER markprey_ro@'%' ;
DROP USER mkp_reporter@localhost ;
DROP USER mkp_loader@localhost ;

--
-- reload all the privileges
FLUSH PRIVILEGES ;
