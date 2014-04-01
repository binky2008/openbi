CREATE OR REPLACE VIEW stag_stat_v
AS
   SELECT   sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , ob.stag_package_name
          , st.stag_partition
          , st.stag_load_id
          , ty.stag_stat_type_name
          , st.stag_id
          , st.stag_stat_value
          , st.stag_stat_error
          , st.create_date AS stat_start
          , st.update_date AS stat_finish
          , NUMTODSINTERVAL (ROUND ((st.update_date - st.create_date) * 86400), 'second') AS stat_duration
          , st.stag_stat_sid
       FROM stag_stat_t st
          , stag_stat_type_t ty
          , stag_object_t ob
          , stag_source_t sc
      WHERE st.stag_stat_type_id = ty.stag_stat_type_id
        AND st.stag_object_id = ob.stag_object_id
        AND ob.stag_source_id = sc.stag_source_id
   ORDER BY st.update_date DESC
          , st.create_date DESC
          , st.stag_stat_id;

COMMENT ON TABLE stag_stat_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stag_stat_v TO PUBLIC;