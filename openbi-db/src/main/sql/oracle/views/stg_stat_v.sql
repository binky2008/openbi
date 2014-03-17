CREATE OR REPLACE VIEW stg_stat_v
AS
   SELECT   sc.stg_source_code
          , ob.stg_object_id
          , ob.stg_object_name
          , ob.stg_package_name
          , st.stg_partition
          , st.stg_load_id
          , ty.stg_stat_type_name
          , st.stg_id
          , st.stg_stat_value
          , st.stg_stat_error
          , st.create_date AS stat_start
          , st.update_date AS stat_finish
          , NUMTODSINTERVAL (ROUND ((st.update_date - st.create_date) * 86400), 'second') AS stat_duration
          , st.stg_stat_sid
       FROM stg_stat_t st
          , stg_stat_type_t ty
          , stg_object_t ob
          , stg_source_t sc
      WHERE st.stg_stat_type_id = ty.stg_stat_type_id
        AND st.stg_object_id = ob.stg_object_id
        AND ob.stg_source_id = sc.stg_source_id
   ORDER BY st.update_date DESC
          , st.create_date DESC
          , st.stg_stat_id;

COMMENT ON TABLE stg_stat_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_stat_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_stat_v'
                                 , 'stg_stat_v'
                                 , TRUE
                                  );
END;
/