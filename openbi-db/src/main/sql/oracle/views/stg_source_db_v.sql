CREATE OR REPLACE VIEW stg_source_db_v
AS
   SELECT   sc.stg_source_id
          , sc.stg_source_code
          , db.stg_source_db_link
          , db.stg_source_db_jdbcname
          , db.stg_source_owner
          , db.stg_distribution_code
          , db.stg_source_bodi_ds
          , db.update_date
       FROM stg_source_db_t db
          , stg_source_t sc
      WHERE sc.stg_source_id = db.stg_source_id
   ORDER BY sc.stg_source_code;

COMMENT ON TABLE stg_source_db_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_source_db_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_source_db_v'
                                 , 'stg_source_db_v'
                                 , TRUE
                                  );
END;
/