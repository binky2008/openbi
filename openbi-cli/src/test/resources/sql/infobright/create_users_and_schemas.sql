-- TEST
drop user test;

create user test@`localhost` identified by 'test';
create user test@`%` identified by 'test';

grant all privileges on test.* to test;


-- DWHSTAGE
drop user dwhstage;

create user dwhstage identified by 'dwhstage';

drop database if exists dwhstage;

create database dwhstage;

grant all privileges on dwhstage.* to dwhstage;


-- DWHREPORT
drop user dwhreport;

create user dwhreport identified by 'dwhreport';

drop database if exists dwhreport;

create database dwhreport;

grant all privileges on dwhreport.* to dwhreport;