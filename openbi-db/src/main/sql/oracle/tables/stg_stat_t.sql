SET SERVEROUTPUT ON;

BEGIN
   ddl.prc_create_entity (
      'stg_stat',
      'stg_id NUMBER,
			  stg_object_id NUMBER,
			  stg_partition NUMBER,
			  stg_load_id NUMBER,
			  stg_stat_type_id NUMBER,
			  stg_stat_value NUMBER,
			  stg_stat_error NUMBER,
			  stg_stat_sid NUMBER',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE stg_stat_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';