-- SugarCRM
drop user sugarcrm;

create user sugarcrm identified by 'sugarcrm';

drop database if exists sugarcrm;

create database sugarcrm;

grant all privileges on sugarcrm.* to sugarcrm;

-- DWHSTAGE
drop user dwhstage;

create user dwhstage identified by 'dwhstage';

drop database if exists dwhstage;

create database dwhstage;

grant all privileges on dwhstage.* to dwhstage;

commit;