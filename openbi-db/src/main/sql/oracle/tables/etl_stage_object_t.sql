SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
      ('etl_stage_object'
     , 'etl_stage_source_id NUMBER,
		etl_stage_object_name VARCHAR2 (100),
        etl_stage_object_comment VARCHAR2(4000),
		etl_stage_object_root VARCHAR2 (100),
		etl_stage_src_table_name VARCHAR2 (100),
		etl_stage_stg1_table_name VARCHAR2 (100),
		etl_stage_stg2_table_name VARCHAR2 (100),
		etl_stage_stg2_nk_name VARCHAR2 (100),
		etl_stage_stg2_view_name VARCHAR2 (100),
		etl_stage_stg2_hist_name VARCHAR2 (100),
		etl_stage_diff_table_name VARCHAR2 (100),
		etl_stage_diff_nk_name VARCHAR2 (100),
		etl_stage_dupl_table_name VARCHAR2 (100),
		etl_stage_package_name VARCHAR2 (100),
		etl_stage_source_nk_flag NUMBER,
		etl_stage_parallel_degree NUMBER DEFAULT 1,
        etl_stage_filter_clause VARCHAR2(4000),
        etl_stage_increment_buffer NUMBER,
        etl_stage_partition_clause VARCHAR2(4000),
        etl_stage_delta_flag NUMBER DEFAULT 0,
        etl_stage_fbda_flag NUMBER DEFAULT 0,
        etl_stage_std_load_modus VARCHAR2(10) DEFAULT ''F'''
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

ALTER TABLE etl_stage_object_t ADD (CONSTRAINT etl_stage_object_uk UNIQUE ( etl_stage_object_root));

COMMENT ON TABLE etl_stage_object_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';