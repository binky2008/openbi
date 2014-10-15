-- TEST
drop schema testdb.test cascade;

create schema testdb.test authorization test;

grant list on testdb to test;


-- SugarCRM
drop schema testdb.sugarcrm cascade;

create schema testdb.sugarcrm authorization sugarcrm;

grant list on testdb to sugarcrm;