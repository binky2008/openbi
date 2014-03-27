SET SERVEROUTPUT ON;

BEGIN
   ddls.prc_create_entity (
      'stag_column_check',
      'stag_object_id NUMBER,
		stag_column_name VARCHAR2 (100),
		stag_column_pos NUMBER,
		stag_column_def VARCHAR2 (100),
		stag_column_nk_pos NUMBER',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE stag_column_check_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';