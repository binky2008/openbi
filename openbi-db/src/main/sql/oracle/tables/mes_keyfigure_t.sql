SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
         ('mes_keyfigure'
        , 'mes_query_id NUMBER,
           mes_keyfigure_code VARCHAR2(100) NOT NULL,
		       mes_keyfigure_name VARCHAR2(1000)'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mes_keyfigure_uk ON mes_keyfigure_t (mes_query_id,mes_keyfigure_code);

COMMENT ON TABLE mes_keyfigure_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';