-- User TEST
CREATE USER test IDENTIFIED BY 'test'
GRANT ROLE PUBLIC TO test WITH NO ADMIN OPTION;
GRANT create table to test WITH NO ADMIN OPTION; 
create schema authorization TEST;

-- User SUGARCRM
CREATE USER sugarcrm IDENTIFIED BY 'sugarcrm'
GRANT ROLE PUBLIC TO sugarcrm WITH NO ADMIN OPTION;
GRANT create table to sugarcrm WITH NO ADMIN OPTION; 
create schema authorization sugarcrm;

-- User DWHSTAGE
CREATE USER dwhstage IDENTIFIED BY 'dwhstage'
GRANT ROLE PUBLIC TO dwhstage WITH NO ADMIN OPTION;
GRANT create table to dwhstage WITH NO ADMIN OPTION; 
create schema authorization dwhstage;