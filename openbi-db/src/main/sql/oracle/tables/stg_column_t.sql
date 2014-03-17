SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
      ('stg_column'
     , 'stg_object_id NUMBER,
		stg_column_pos NUMBER,
		stg_column_name VARCHAR2 (100),
		stg_column_name_map VARCHAR2 (100),
		stg_column_comment VARCHAR2 (4000),
		stg_column_def VARCHAR2 (100),
		stg_column_def_src VARCHAR2 (100),
		stg_column_nk_pos NUMBER,
		stg_column_incr_flag NUMBER,
		stg_column_hist_flag NUMBER,
		stg_column_edwh_flag NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE stg_column_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';