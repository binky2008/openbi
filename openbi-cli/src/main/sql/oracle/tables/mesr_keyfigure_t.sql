SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
         ('mesr_keyfigure'
        , 'mesr_query_id NUMBER,
           mesr_keyfigure_code VARCHAR2(100) NOT NULL,
		       mesr_keyfigure_name VARCHAR2(1000)'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mesr_keyfigure_uk ON mesr_keyfigure_t (mesr_query_id,mesr_keyfigure_code);

COMMENT ON TABLE mesr_keyfigure_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';