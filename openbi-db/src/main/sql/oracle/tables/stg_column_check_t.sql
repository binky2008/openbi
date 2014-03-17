SET SERVEROUTPUT ON;

BEGIN
   ddl.prc_create_entity (
      'stg_column_check',
      'stg_object_id NUMBER,
		stg_column_name VARCHAR2 (100),
		stg_column_pos NUMBER,
		stg_column_def VARCHAR2 (100),
		stg_column_nk_pos NUMBER',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE stg_column_check_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';