SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
      ('stg_object'
     , 'stg_source_id NUMBER,
		stg_object_name VARCHAR2 (100),
        stg_object_comment VARCHAR2(4000),
		stg_object_root VARCHAR2 (100),
		stg_src_table_name VARCHAR2 (100),
		stg_stg1_table_name VARCHAR2 (100),
		stg_stg2_table_name VARCHAR2 (100),
		stg_stg2_nk_name VARCHAR2 (100),
		stg_stg2_view_name VARCHAR2 (100),
		stg_stg2_hist_name VARCHAR2 (100),
		stg_diff_table_name VARCHAR2 (100),
		stg_diff_nk_name VARCHAR2 (100),
		stg_dupl_table_name VARCHAR2 (100),
		stg_package_name VARCHAR2 (100),
		stg_source_nk_flag NUMBER,
		stg_parallel_degree NUMBER DEFAULT 1,
        stg_filter_clause VARCHAR2(4000),
        stg_increment_buffer NUMBER,
        stg_partition_clause VARCHAR2(4000),
        stg_delta_flag NUMBER DEFAULT 0,
        stg_fbda_flag NUMBER DEFAULT 0,
        stg_std_load_modus VARCHAR2(10) DEFAULT ''F'''
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

ALTER TABLE stg_object_t ADD (CONSTRAINT stg_object_uk UNIQUE ( stg_object_root));

COMMENT ON TABLE stg_object_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';