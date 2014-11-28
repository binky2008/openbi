CREATE OR REPLACE VIEW p#frm#stag_object_v
AS
     SELECT sc.stag_source_id
          , sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , ob.stag_object_comment
          , ob.stag_object_root
          , ob.stag_src_table_name
          , ob.stag_stage_table_name
          , ob.stag_hist_table_name
          , ob.stag_hist_nk_name
          , ob.stag_dupl_table_name
          , ob.stag_diff_table_name
          , ob.stag_diff_nk_name
          , ob.stag_hist_view_name
          , ob.stag_hist_fbda_name
          , ob.stag_package_name
          , ob.stag_source_nk_flag
          , ob.stag_parallel_degree
          , ob.stag_partition_clause
          , ob.stag_filter_clause
          , ob.stag_fbda_flag
          , ob.update_date
       FROM p#frm#stag_object_t ob
          , p#frm#stag_source_t sc
      WHERE ob.stag_source_id = sc.stag_source_id
   ORDER BY sc.stag_source_code
          , ob.stag_object_name;