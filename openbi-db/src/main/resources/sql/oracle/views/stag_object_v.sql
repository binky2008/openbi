CREATE OR REPLACE VIEW stag_object_v
AS
     SELECT sc.stag_source_id
          , sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , ob.stag_object_comment
          , ob.stag_object_root
          , ob.stag_src_table_name
          , ob.stag_stg1_table_name
          , ob.stag_stg2_table_name
          , ob.stag_stg2_nk_name
          , ob.stag_dupl_table_name
          , ob.stag_diff_table_name
          , ob.stag_diff_nk_name
          , ob.stag_stg2_view_name
          , ob.stag_stg2_hist_name
          , ob.stag_package_name
          , ob.stag_source_nk_flag
          , ob.stag_parallel_degree
          , ob.stag_partition_clause
          , ob.stag_filter_clause
          , ob.stag_fbda_flag
          , stag_std_load_modus
          , ob.update_date
       FROM stag_object_t ob
          , stag_source_t sc
      WHERE ob.stag_source_id = sc.stag_source_id
   ORDER BY sc.stag_source_code
          , ob.stag_object_name;