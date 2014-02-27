SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
      ('etl_stage_column'
     , 'etl_stage_object_id NUMBER,
		etl_stage_column_pos NUMBER,
		etl_stage_column_name VARCHAR2 (100),
		etl_stage_column_name_map VARCHAR2 (100),
		etl_stage_column_comment VARCHAR2 (4000),
		etl_stage_column_def VARCHAR2 (100),
		etl_stage_column_def_src VARCHAR2 (100),
		etl_stage_column_nk_pos NUMBER,
		etl_stage_column_incr_flag NUMBER,
		etl_stage_column_hist_flag NUMBER,
		etl_stage_column_edwh_flag NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE etl_stage_column_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';