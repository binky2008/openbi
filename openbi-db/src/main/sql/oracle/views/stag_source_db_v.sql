CREATE OR REPLACE VIEW stag_source_db_v
AS
   SELECT   sc.stag_source_id
          , sc.stag_source_code
          , db.stag_source_db_link
          , db.stag_source_db_jdbcname
          , db.stag_source_owner
          , db.stag_distribution_code
          , db.stag_source_bodi_ds
          , db.update_date
       FROM stag_source_db_t db
          , stag_source_t sc
      WHERE sc.stag_source_id = db.stag_source_id
   ORDER BY sc.stag_source_code;

COMMENT ON TABLE stag_source_db_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stag_source_db_v TO PUBLIC;