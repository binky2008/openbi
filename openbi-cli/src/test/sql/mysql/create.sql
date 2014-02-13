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

use test;

drop table if exists tab_test;

create table tab_test (
    col_boolean boolean
  , col_bool bool
  , col_bit bit
  , col_tinyint tinyint
  , col_smallint smallint
  , col_mediumint mediumint
  , col_int int
  , col_integer integer
  , col_bigint bigint
  , col_serial serial
  , col_decimal decimal (50,10)
  , col_dec dec (50,10)
  , col_double double (255,20)
  , col_double_precision double precision (255,20)
  , col_float float (255,20)
  , col_date date
  , col_time time
  , col_datetime datetime
  , col_timestamp timestamp
  , col_year year
  , col_char char(240)
  , col_varchar varchar(200)
  , col_binary binary (255)
  , col_varbinary varbinary (240)
  , col_tinyblob tinyblob
  , col_blob blob
  , col_mediumblob mediumblob
  , col_longblob longblob
  , col_tinytext tinytext
  , col_text text
  , col_mediumtext mediumtext
  , col_longtext longtext
  , col_enum enum ('a','b','c')
  , col_set set ('a','b','c')
);