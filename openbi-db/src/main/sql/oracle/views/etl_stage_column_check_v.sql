CREATE OR REPLACE VIEW etl_stage_column_check_v
AS
   SELECT   sc.etl_stage_source_id
          , sc.etl_stage_source_code
          , ob.etl_stage_object_id
          , ob.etl_stage_object_name
          , ob.etl_stage_source_nk_flag
          , co.etl_stage_column_id
          , co.etl_stage_column_name
          , co.etl_stage_column_name_map
          , co.etl_stage_column_comment
          , co.etl_stage_column_edwh_flag
          , co.etl_stage_column_stg_pos
          , co.etl_stage_column_stg_def
          , co.etl_stage_column_stg_nk_pos
          , co.etl_stage_column_src_pos
          , co.etl_stage_column_src_def
          , co.etl_stage_column_src_nk_pos
          , co.update_date
       FROM (SELECT NVL (c.etl_stage_object_id, k.etl_stage_object_id) AS etl_stage_object_id
                  , c.etl_stage_column_id
                  , NVL (c.etl_stage_column_name, k.etl_stage_column_name) AS etl_stage_column_name
                  , c.etl_stage_column_name_map
                  , c.etl_stage_column_comment
                  , c.etl_stage_column_edwh_flag
                  , c.etl_stage_column_pos AS etl_stage_column_stg_pos
                  , c.etl_stage_column_def AS etl_stage_column_stg_def
                  , c.etl_stage_column_nk_pos AS etl_stage_column_stg_nk_pos
                  , k.etl_stage_column_pos AS etl_stage_column_src_pos
                  , k.etl_stage_column_def AS etl_stage_column_src_def
                  , k.etl_stage_column_nk_pos AS etl_stage_column_src_nk_pos
                  , c.update_date
               FROM etl_stage_column_check_t k FULL OUTER JOIN etl_stage_column_t c ON c.etl_stage_object_id = k.etl_stage_object_id
                                                                                  AND c.etl_stage_column_name = k.etl_stage_column_name
                    ) co
          , etl_stage_object_t ob
          , etl_stage_source_t sc
      WHERE ob.etl_stage_object_id = co.etl_stage_object_id
        AND ob.etl_stage_source_id = sc.etl_stage_source_id
   ORDER BY sc.etl_stage_source_code
          , ob.etl_stage_object_name
          , NVL (co.etl_stage_column_stg_pos, co.etl_stage_column_src_pos);

COMMENT ON TABLE etl_stage_column_check_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';


GRANT SELECT ON etl_stage_column_check_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_column_check_v'
                                 , 'etl_stage_column_check_v'
                                 , TRUE
                                  );
END;
/