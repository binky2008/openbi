SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
      ('stag_column'
     , 'stag_object_id NUMBER,
		stag_column_pos NUMBER,
		stag_column_name VARCHAR2 (100),
		stag_column_name_map VARCHAR2 (100),
		stag_column_comment VARCHAR2 (4000),
		stag_column_def VARCHAR2 (100),
		stag_column_def_src VARCHAR2 (100),
		stag_column_nk_pos NUMBER,
		stag_column_incr_flag NUMBER,
		stag_column_hist_flag NUMBER,
		stag_column_edwh_flag NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE stag_column_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';