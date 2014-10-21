-- TEST
drop schema testdb.test cascade;

drop user test;

drop schema testdb.test cascade;

create schema testdb.test authorization test;

grant list on testdb to test;


-- SugarCRM
drop schema testdb.sugarcrm cascade;

drop user sugarcrm;

drop schema testdb.sugarcrm cascade;

create schema testdb.sugarcrm authorization sugarcrm;

grant list on testdb to sugarcrm;


-- DWHStage
drop schema testdb.dwhstage cascade;

drop user dwhstage;

create user dwhstage with password 'dwhstage';

create schema testdb.dwhstage authorization dwhstage;

grant list on testdb to dwhstage;