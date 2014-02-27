SET SERVEROUTPUT ON;

BEGIN
   utl_ddl.prc_create_object_standard (
      'etl_stage_column_check',
      'etl_stage_object_id NUMBER,
		etl_stage_column_name VARCHAR2 (100),
		etl_stage_column_pos NUMBER,
		etl_stage_column_def VARCHAR2 (100),
		etl_stage_column_nk_pos NUMBER',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE etl_stage_column_check_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';