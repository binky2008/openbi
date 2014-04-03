BEGIN
   ddls.prc_create_entity (
      'stag_object'
    , 'stag_source_id NUMBER,
	   stag_object_name VARCHAR2 (100),
       stag_object_comment VARCHAR2(4000),
	   stag_object_root VARCHAR2 (100),
	   stag_src_table_name VARCHAR2 (100),
	   stag_stg1_table_name VARCHAR2 (100),
	   stag_stg2_table_name VARCHAR2 (100),
	   stag_stg2_nk_name VARCHAR2 (100),
	   stag_stg2_view_name VARCHAR2 (100),
	   stag_stg2_hist_name VARCHAR2 (100),
       stag_diff_table_name VARCHAR2 (100),
	   stag_diff_nk_name VARCHAR2 (100),
	   stag_dupl_table_name VARCHAR2 (100),
	   stag_package_name VARCHAR2 (100),
	   stag_source_nk_flag NUMBER,
	   stag_parallel_degree NUMBER DEFAULT 1,
       stag_filter_clause VARCHAR2(4000),
       stag_increment_buffer NUMBER,
       stag_partition_clause VARCHAR2(4000),
       stag_delta_flag NUMBER DEFAULT 0,
       stag_fbda_flag NUMBER DEFAULT 0,
       stag_std_load_modus VARCHAR2(10) DEFAULT ''F'''
    , 'DROP'
    , TRUE
    , TRUE
   );
END;