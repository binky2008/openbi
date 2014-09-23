use test;

drop table if exists tab_test;

create table tab_test (
    col_boolean boolean
  , col_bool bool
  --
  , col_bit bit(10)
  , col_tinyint tinyint
  , col_smallint smallint
  , col_mediumint mediumint
  , col_int int
  , col_integer integer
  , col_bigint bigint
  , col_serial serial
  --
  , col_decimal decimal (50,10)
  , col_dec dec (50,10)
  , col_double double (255,20)
  , col_double_precision double precision (255,20)
  , col_float float (255,20)
  --
  , col_date date
  , col_time time
  , col_datetime datetime
  , col_timestamp timestamp
  , col_year year
  --
  , col_char char(240)
  , col_varchar varchar(200)
  , col_tinytext tinytext
  , col_text text
  , col_mediumtext mediumtext
  , col_longtext longtext
  --
  , col_binary binary (10)
  , col_varbinary varbinary (240)
  , col_tinyblob tinyblob
  , col_blob blob
  , col_mediumblob mediumblob
  , col_longblob longblob
  --
  , col_enum enum ('a','b','c')
  , col_set set ('a','b','c')
);