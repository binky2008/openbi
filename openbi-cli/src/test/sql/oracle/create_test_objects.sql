DROP TABLE tab_test;

CREATE TABLE tab_test (
	col_char char(267)
  , col_nchar nchar(58)
  , col_varchar2 varchar2(3215)
  , col_nvarchar2 nvarchar2(325)
  , col_INTEGER INTEGER
  , col_smallint smallint
  , col_decimal decimal(32,7)
  , col_numeric numeric(32,7)
  , col_DOUBLE DOUBLE precision
  , col_FLOAT FLOAT
  , col_real real
  , col_BINARY_DOUBLE BINARY_DOUBLE
  , col_BINARY_FLOAT BINARY_FLOAT
  , col_DATE date
  , col_timestamp timestamp
  , col_timestamptz timestamp with time zone
  , col_timestampltz TIMESTAMP WITH LOCAL TIME ZONE
  , col_intervalds INTERVAL DAY TO SECOND
  , col_intervalym INTERVAL YEAR TO MONTH
  , col_BFILE BFILE
  , col_BLOB BLOB
  , col_CLOB CLOB
  , col_NCLOB NCLOB
  , col_LONGRAW LONG RAW
  , col_ROWID ROWID
  , col_UROWID UROWID
  , col_xmltype xmltype
  , col_SDO_GEOMETRY SDO_GEOMETRY
  , col_SDO_RASTER SDO_RASTER
);