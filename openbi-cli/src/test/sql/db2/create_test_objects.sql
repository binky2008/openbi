DROP TABLE test.tab_test;
--
CREATE TABLE test.tab_test(
	col_char CHAR(215) ,
	col_varchar VARCHAR(21000) ,
    col_clob CLOB(1521257) ,
    col_graphic GRAPHIC(78) ,
    col_vargraphic VARGRAPHIC(3453) ,
    col_DBCLOB DBCLOB(541325413) ,
    col_blob BLOB(215741641)
);