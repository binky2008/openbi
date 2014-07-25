DROP TABLE test.tab_test;
--
CREATE TABLE test.tab_test (
	col_boolean BOOLEAN ,
	--
	col_smallint SMALLINT ,
	col_integer integer ,
	col_BIGINT BIGINT ,
	col_DECIMAL DECIMAL(28,8) ,
	col_NUMERIC NUMERIC(28,8) ,
	col_REAL REAL ,
	col_FLOAT FLOAT ,
	col_DOUBLE DOUBLE ,
	--
	col_date date ,
	col_time time ,
	col_timestamp timestamp ,
	--
	col_char CHAR(215) ,
	col_varchar VARCHAR(21000) ,
	col_longvarchar LONG VARCHAR ,
    col_clob CLOB(1521257) ,
	--
	col_charbd CHAR (215) FOR BIT DATA,
	col_varcharbd VARCHAR(21000) FOR BIT DATA,
	col_longvarcharbd LONG VARCHAR FOR BIT DATA ,
    col_blob BLOB(215741641),
    --
    col_xml xml
);