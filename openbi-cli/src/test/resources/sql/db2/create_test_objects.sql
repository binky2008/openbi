DROP TABLE test.tab_test;
--
CREATE TABLE test.tab_test(
	col_smallint SMALLINT ,
	col_integer integer ,
	col_BIGINT BIGINT ,
	col_NUMERIC NUMERIC(28,8) ,
	col_DECFLOAT DECFLOAT(34) ,
	col_REAL REAL ,
	col_DOUBLE DOUBLE ,
	--
	col_date date ,
	col_time time ,
	col_timestamp timestamp,
	--
	col_char CHAR(215) ,
	col_varchar VARCHAR(21000) ,
    col_clob CLOB(1521257) ,
    col_DBCLOB DBCLOB(541325413) ,
    col_xml xml ,
    --
    col_graphic GRAPHIC(78) ,
    col_vargraphic VARGRAPHIC(3453) ,
    col_blob BLOB(215741641)
);