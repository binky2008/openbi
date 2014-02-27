CREATE OR REPLACE VIEW etl_stage_source_v
AS
   SELECT etl_stage_source_id
        , etl_stage_source_code
        , etl_stage_source_name
        , etl_stage_source_prefix
        , etl_stage_owner
        , etl_stage_ts_stg1_data
        , etl_stage_ts_stg1_indx
        , etl_stage_ts_stg2_data
        , etl_stage_ts_stg2_indx
        , etl_stage_fb_archive
        , etl_stage_bodi_ds
        , etl_stage_source_bodi_ds
        , update_date
     FROM etl_stage_source_t;

COMMENT ON TABLE etl_stage_source_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_source_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_source_v'
                                 , 'etl_stage_source_v'
                                 , TRUE
                                  );
END;
/