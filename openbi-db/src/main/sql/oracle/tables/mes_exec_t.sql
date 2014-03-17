SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
         ('mes_exec'
        , 'mes_keyfigure_id NUMBER,
		       mes_exec_result_value NUMBER,
		       mes_exec_result_report CLOB'
         , 'DROP'
	       , TRUE
         , TRUE
         );
END;
/

COMMENT ON TABLE mes_exec_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';