CREATE OR REPLACE VIEW stag_source_v
AS
   SELECT stag_source_id
        , stag_source_code
        , stag_source_name
        , stag_source_prefix
        , stag_owner
        , stag_ts_stg1_data
        , stag_ts_stg1_indx
        , stag_ts_stg2_data
        , stag_ts_stg2_indx
        , stag_fb_archive
        , stag_bodi_ds
        , stag_source_bodi_ds
        , update_date
     FROM stag_source_t;

COMMENT ON TABLE stag_source_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stag_source_v TO PUBLIC;