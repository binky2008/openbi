SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
         ('mesr_query'
        , 'mesr_query_code VARCHAR2(100) NOT NULL,
		       mesr_query_name VARCHAR2(1000),
		       mesr_query_sql CLOB'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mesr_query_uk ON mesr_query_t (mesr_query_code);

COMMENT ON TABLE mesr_query_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';