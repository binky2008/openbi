SET SERVEROUTPUT ON;

BEGIN
   utl_ddl.prc_create_object_standard (
      'etl_stage_stat',
      'etl_stage_id NUMBER,
			  etl_stage_object_id NUMBER,
			  etl_stage_partition NUMBER,
			  etl_stage_load_id NUMBER,
			  etl_stage_stat_type_id NUMBER,
			  etl_stage_stat_value NUMBER,
			  etl_stage_stat_error NUMBER,
			  etl_stage_stat_gui NUMBER,
			  etl_stage_stat_sid NUMBER',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE etl_stage_stat_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';