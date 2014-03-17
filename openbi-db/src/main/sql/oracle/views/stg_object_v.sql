CREATE OR REPLACE VIEW stg_object_v
AS
   SELECT   sc.stg_source_id
          , sc.stg_source_code
          , ob.stg_object_id
          , ob.stg_object_name
          , ob.stg_object_comment
          , ob.stg_object_root
          , ob.stg_src_table_name
          , ob.stg_stg1_table_name
          , ob.stg_stg2_table_name
          , ob.stg_stg2_nk_name
          , ob.stg_dupl_table_name
          , ob.stg_diff_table_name
          , ob.stg_diff_nk_name
          , ob.stg_stg2_view_name
          , ob.stg_stg2_hist_name
          , ob.stg_package_name
          , ob.stg_source_nk_flag
          , ob.stg_parallel_degree
          , ob.stg_partition_clause
          , ob.stg_filter_clause
          , ob.stg_fbda_flag
          , stg_std_load_modus
          , ob.update_date
       FROM stg_object_t ob
          , stg_source_t sc
      WHERE ob.stg_source_id = sc.stg_source_id
   ORDER BY sc.stg_source_code
          , ob.stg_object_name;

COMMENT ON TABLE stg_object_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_object_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_object_v'
                                 , 'stg_object_v'
                                 , TRUE
                                  );
END;
/