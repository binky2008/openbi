CREATE OR REPLACE VIEW stg_column_check_v
AS
   SELECT   sc.stg_source_id
          , sc.stg_source_code
          , ob.stg_object_id
          , ob.stg_object_name
          , ob.stg_source_nk_flag
          , co.stg_column_id
          , co.stg_column_name
          , co.stg_column_name_map
          , co.stg_column_comment
          , co.stg_column_edwh_flag
          , co.stg_column_stg_pos
          , co.stg_column_stg_def
          , co.stg_column_stg_nk_pos
          , co.stg_column_src_pos
          , co.stg_column_src_def
          , co.stg_column_src_nk_pos
          , co.update_date
       FROM (SELECT NVL (c.stg_object_id, k.stg_object_id) AS stg_object_id
                  , c.stg_column_id
                  , NVL (c.stg_column_name, k.stg_column_name) AS stg_column_name
                  , c.stg_column_name_map
                  , c.stg_column_comment
                  , c.stg_column_edwh_flag
                  , c.stg_column_pos AS stg_column_stg_pos
                  , c.stg_column_def AS stg_column_stg_def
                  , c.stg_column_nk_pos AS stg_column_stg_nk_pos
                  , k.stg_column_pos AS stg_column_src_pos
                  , k.stg_column_def AS stg_column_src_def
                  , k.stg_column_nk_pos AS stg_column_src_nk_pos
                  , c.update_date
               FROM stg_column_check_t k FULL OUTER JOIN stg_column_t c ON c.stg_object_id = k.stg_object_id
                                                                                  AND c.stg_column_name = k.stg_column_name
                    ) co
          , stg_object_t ob
          , stg_source_t sc
      WHERE ob.stg_object_id = co.stg_object_id
        AND ob.stg_source_id = sc.stg_source_id
   ORDER BY sc.stg_source_code
          , ob.stg_object_name
          , NVL (co.stg_column_stg_pos, co.stg_column_src_pos);

COMMENT ON TABLE stg_column_check_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';


GRANT SELECT ON stg_column_check_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_column_check_v'
                                 , 'stg_column_check_v'
                                 , TRUE
                                  );
END;
/