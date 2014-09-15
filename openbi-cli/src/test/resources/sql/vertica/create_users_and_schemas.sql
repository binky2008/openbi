create user test identified by 'vertica';
create schema test authorization test;

create user dwhstage identified by 'vertica';
create schema dwhstage authorization dwhstage;

create user dwhreport identified by 'vertica';
create schema dwhreport authorization dwhreport;