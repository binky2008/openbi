SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
      ('etl_stage_source'
     , 'etl_stage_source_code VARCHAR2(10),
		etl_stage_source_prefix VARCHAR2(10),
		etl_stage_source_name VARCHAR2(1000),
		etl_stage_owner VARCHAR2(100),
		etl_stage_ts_stg1_data VARCHAR2(100),
		etl_stage_ts_stg1_indx VARCHAR2(100),
		etl_stage_ts_stg2_data VARCHAR2(100),
		etl_stage_ts_stg2_indx VARCHAR2(100),
		etl_stage_fb_archive VARCHAR2(100),
		etl_stage_bodi_ds VARCHAR2(100),
		etl_stage_source_bodi_ds VARCHAR2(100)'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE etl_stage_source_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';