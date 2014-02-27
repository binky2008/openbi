CREATE OR REPLACE VIEW etl_stage_stat_v
AS
   SELECT   sc.etl_stage_source_code
          , ob.etl_stage_object_id
          , ob.etl_stage_object_name
          , ob.etl_stage_package_name
          , st.etl_stage_partition
          , st.etl_stage_load_id
          , ty.etl_stage_stat_type_name
          , st.etl_stage_id
          , st.etl_stage_stat_gui
          , st.etl_stage_stat_value
          , st.etl_stage_stat_error
          , st.create_date AS stat_start
          , st.update_date AS stat_finish
          , NUMTODSINTERVAL (ROUND ((st.update_date - st.create_date) * 86400), 'second') AS stat_duration
          , st.etl_stage_stat_sid
       FROM etl_stage_stat_t st
          , etl_stage_stat_type_t ty
          , etl_stage_object_t ob
          , etl_stage_source_t sc
      WHERE st.etl_stage_stat_type_id = ty.etl_stage_stat_type_id
        AND st.etl_stage_object_id = ob.etl_stage_object_id
        AND ob.etl_stage_source_id = sc.etl_stage_source_id
   ORDER BY st.update_date DESC
          , st.create_date DESC
          , st.etl_stage_stat_id;

COMMENT ON TABLE etl_stage_stat_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_stat_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_stat_v'
                                 , 'etl_stage_stat_v'
                                 , TRUE
                                  );
END;
/