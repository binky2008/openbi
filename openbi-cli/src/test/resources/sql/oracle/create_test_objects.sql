DROP TABLE tab_test;

CREATE TABLE tab_test (
    col_INTEGER INTEGER
  , col_smallint smallint
  , col_decimal decimal(32,7)
  , col_numeric numeric(32,7)
  , col_DOUBLE DOUBLE precision
  , col_FLOAT FLOAT (65)
  , col_real real
  , col_BINARY_DOUBLE BINARY_DOUBLE
  , col_BINARY_FLOAT BINARY_FLOAT
  --
  , col_DATE date
  , col_timestamp timestamp
  , col_timestamptz timestamp with time zone
  , col_timestampltz TIMESTAMP WITH LOCAL TIME ZONE
  , col_intervalds INTERVAL DAY TO SECOND
  , col_intervalym INTERVAL YEAR TO MONTH
  --
  , col_char char(267)
  , col_varchar2 varchar2(3215)
  , col_CLOB CLOB
  , col_nchar nchar(58)
  , col_nvarchar2 nvarchar2(325)
  , col_NCLOB NCLOB
  --
  , col_BFILE BFILE
  , col_BLOB BLOB
  , col_LONGRAW LONG RAW
  --
  , col_ROWID ROWID
  , col_UROWID UROWID
  , col_xmltype xmltype
  , col_SDO_GEOMETRY SDO_GEOMETRY
  , col_SDO_RASTER SDO_RASTER
);

DROP TABLE bigtable;
CREATE TABLE bigtable (
   col_pk       NUMBER
 , col_number   NUMBER
 , col_text     VARCHAR2 (4000)
 , CONSTRAINT bigtable_pk PRIMARY KEY (col_pk)
);

DECLARE
   l_n_rows       NUMBER := 10000;
   l_vc_bigtext   VARCHAR2 (4000);
BEGIN
   FOR i IN 1 .. l_n_rows LOOP
      INSERT INTO bigtable
           VALUES (
                     i
                   , DBMS_RANDOM.random
                   , DBMS_RANDOM.string (
                        'a'
                      , TRUNC (DBMS_RANDOM.VALUE (
                                  1
                                , 4000
                               ))
                     )
                  );
   END LOOP;

   COMMIT;
END;
/

DROP TABLE smalltable;
CREATE TABLE smalltable (
   col_pk      	 	NUMBER
 , col_number   	NUMBER
 , col_text     	VARCHAR2 (4000)
 , col_othertext   	VARCHAR2 (4000)
 , CONSTRAINT smalltable_pk PRIMARY KEY (col_pk)
);