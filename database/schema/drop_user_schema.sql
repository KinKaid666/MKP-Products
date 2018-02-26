--
-- switch to the user session
use usertable ;

--
-- drop the tables
DROP TABLE IF EXISTS users ;
DROP TABLE IF EXISTS user_cookies ;
DROP TABLE IF EXISTS user_views ;

--
-- drop the database
DROP DATABASE IF EXISTS usertable ;
