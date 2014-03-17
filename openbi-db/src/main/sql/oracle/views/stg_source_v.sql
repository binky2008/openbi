CREATE OR REPLACE VIEW stg_source_v
AS
   SELECT stg_source_id
        , stg_source_code
        , stg_source_name
        , stg_source_prefix
        , stg_owner
        , stg_ts_stg1_data
        , stg_ts_stg1_indx
        , stg_ts_stg2_data
        , stg_ts_stg2_indx
        , stg_fb_archive
        , stg_bodi_ds
        , stg_source_bodi_ds
        , update_date
     FROM stg_source_t;

COMMENT ON TABLE stg_source_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_source_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_source_v'
                                 , 'stg_source_v'
                                 , TRUE
                                  );
END;
/