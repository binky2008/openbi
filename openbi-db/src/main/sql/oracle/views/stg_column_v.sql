CREATE OR REPLACE VIEW stg_column_v
AS
   SELECT   sc.stg_source_id
          , sc.stg_source_code
          , ob.stg_object_id
          , ob.stg_object_name
          , ob.stg_source_nk_flag
          , co.stg_column_id
          , co.stg_column_pos
          , co.stg_column_name
          , co.stg_column_name_map
          , co.stg_column_comment
          , co.stg_column_def
          , co.stg_column_def_src
          , co.stg_column_nk_pos
          , co.stg_column_hist_flag
          , co.stg_column_edwh_flag
          , co.update_date
       FROM stg_column_t co
          , stg_object_t ob
          , stg_source_t sc
      WHERE ob.stg_object_id = co.stg_object_id
        AND ob.stg_source_id = sc.stg_source_id
   ORDER BY sc.stg_source_code
          , ob.stg_object_name
          , co.stg_column_pos;

COMMENT ON TABLE stg_column_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';


GRANT SELECT ON stg_column_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_column_v'
                                 , 'stg_column_v'
                                 , TRUE
                                  );
END;
/