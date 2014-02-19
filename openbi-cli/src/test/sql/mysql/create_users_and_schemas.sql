-- SugarCRM
drop user sugarcrm;

create user sugarcrm identified by 'sugarcrm';

drop database if exists sugarcrm;

create database sugarcrm;

grant all privileges on sugarcrm.* to sugarcrm;


-- Magento
drop user magento;

create user magento identified by 'magento';

drop database if exists magento;

create database magento;

grant all privileges on magento.* to magento;


-- DWHSTAGE
drop user dwhstage;

create user dwhstage identified by 'dwhstage';

drop database if exists dwhstage;

create database dwhstage;

grant all privileges on dwhstage.* to dwhstage;

commit;


-- TEST
drop user test;

create user test@`%` identified by 'test';

grant all privileges on test.* to test;
