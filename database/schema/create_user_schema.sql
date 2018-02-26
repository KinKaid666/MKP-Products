--
-- Create the database
CREATE DATABASE IF NOT EXISTS usertable ;

--
-- switch to the user session
use usertable ;

--
-- create schema for users
CREATE TABLE IF NOT EXISTS users
(
    username  VARCHAR(50) NOT NULL -- Channel where order was taken
   ,password  VARCHAR(50) NOT NULL -- Channel where order was taken
   ,status    VARCHAR(50) NOT NULL -- Channel where order was taken
   ,realname  VARCHAR(50) NOT NULL -- Channel where order was taken
   ,email     VARCHAR(50) NOT NULL -- Channel where order was taken
   ,PRIMARY KEY(username)
) ;

--
-- create schema for users
CREATE TABLE IF NOT EXISTS user_cookies
(
    cookie_id     VARCHAR(50)  NOT NULL
   ,username      VARCHAR(50)  NOT NULL
   ,creation_time TIMESTAMP    NOT NULL
   ,remote_ip     VARCHAR(150) NOT NULL
   ,PRIMARY KEY(username)
) ;

--
-- create schema for user viewss
CREATE TABLE IF NOT EXISTS user_views
(
    username      VARCHAR(50)  NOT NULL -- Channel where order was taken
   ,remote_ip     VARCHAR(150) NOT NULL
   ,page          VARCHAR(150) NOT NULL
   ,creation_time TIMESTAMP    NOT NULL
) ;
