CREATE OR REPLACE VIEW etl_stage_column_v
AS
   SELECT   sc.etl_stage_source_id
          , sc.etl_stage_source_code
          , ob.etl_stage_object_id
          , ob.etl_stage_object_name
          , ob.etl_stage_source_nk_flag
          , co.etl_stage_column_id
          , co.etl_stage_column_pos
          , co.etl_stage_column_name
          , co.etl_stage_column_name_map
          , co.etl_stage_column_comment
          , co.etl_stage_column_def
          , co.etl_stage_column_def_src
          , co.etl_stage_column_nk_pos
          , co.etl_stage_column_hist_flag
          , co.etl_stage_column_edwh_flag
          , co.update_date
       FROM etl_stage_column_t co
          , etl_stage_object_t ob
          , etl_stage_source_t sc
      WHERE ob.etl_stage_object_id = co.etl_stage_object_id
        AND ob.etl_stage_source_id = sc.etl_stage_source_id
   ORDER BY sc.etl_stage_source_code
          , ob.etl_stage_object_name
          , co.etl_stage_column_pos;

COMMENT ON TABLE etl_stage_column_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';


GRANT SELECT ON etl_stage_column_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_column_v'
                                 , 'etl_stage_column_v'
                                 , TRUE
                                  );
END;
/