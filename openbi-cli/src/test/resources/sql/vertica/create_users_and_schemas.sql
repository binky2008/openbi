create user test identified by 'vertica';
create schema test authorization test;

drop schema sugarcrm;
drop user sugarcrm;
create user sugarcrm identified by 'vertica';
create schema sugarcrm authorization sugarcrm;

create user dwhstage identified by 'vertica';
create schema dwhstage authorization dwhstage;

create user dwhreport identified by 'vertica';
create schema dwhreport authorization dwhreport;