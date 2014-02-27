CREATE OR REPLACE VIEW etl_stage_object_v
AS
   SELECT   sc.etl_stage_source_id
          , sc.etl_stage_source_code
          , ob.etl_stage_object_id
          , ob.etl_stage_object_name
          , ob.etl_stage_object_comment
          , ob.etl_stage_object_root
          , ob.etl_stage_src_table_name
          , ob.etl_stage_stg1_table_name
          , ob.etl_stage_stg2_table_name
          , ob.etl_stage_stg2_nk_name
          , ob.etl_stage_dupl_table_name
          , ob.etl_stage_diff_table_name
          , ob.etl_stage_diff_nk_name
          , ob.etl_stage_stg2_view_name
          , ob.etl_stage_stg2_hist_name
          , ob.etl_stage_package_name
          , ob.etl_stage_source_nk_flag
          , ob.etl_stage_parallel_degree
          , ob.etl_stage_partition_clause
          , ob.etl_stage_filter_clause
          , ob.etl_stage_fbda_flag
          , etl_stage_std_load_modus
          , ob.update_date
       FROM etl_stage_object_t ob
          , etl_stage_source_t sc
      WHERE ob.etl_stage_source_id = sc.etl_stage_source_id
   ORDER BY sc.etl_stage_source_code
          , ob.etl_stage_object_name;

COMMENT ON TABLE etl_stage_object_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_object_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_object_v'
                                 , 'etl_stage_object_v'
                                 , TRUE
                                  );
END;
/