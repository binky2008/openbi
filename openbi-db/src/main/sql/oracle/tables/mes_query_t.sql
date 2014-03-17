SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
         ('mes_query'
        , 'mes_query_code VARCHAR2(100) NOT NULL,
		       mes_query_name VARCHAR2(1000),
		       mes_query_sql CLOB'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mes_query_uk ON mes_query_t (mes_query_code);

COMMENT ON TABLE mes_query_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';