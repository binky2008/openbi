drop table if exists tab_test;

create table tab_test (
    col_boolean boolean
  
  , col_tinyint tinyint
  , col_integer1 integer1
  , col_smallint smallint
  , col_integer2 integer2
  , col_integer integer
  , col_integer4 integer4
  , col_bigint bigint
  , col_integer8 integer8
  
  , col_decimal decimal
  
  , col_float float (48)
  , col_float4 float4
  , col_float8 float8
  , col_real real
  , col_double double precision
  
  , col_money money
  
  , col_date date
  , col_ansidate ansidate
  , col_time time
  , col_timewotz time without time zone
  , col_timetz time with time zone
  , col_timeltz time with local time zone
  , col_timestamp timestamp
  , col_timestampwotz timestamp without time zone
  , col_timestamptz timestamp with time zone
  , col_timestampltz timestamp with local time zone
  
  , col_intervalym interval year to month
  , col_intervalds interval day to second
  
  , col_char char(100)
  , col_varchar varchar(32000)
  , col_nchar nchar(100)
  , col_nvarchar nvarchar(16000)
  , col_text text
);