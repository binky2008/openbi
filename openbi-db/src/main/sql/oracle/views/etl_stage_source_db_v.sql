CREATE OR REPLACE VIEW etl_stage_source_db_v
AS
   SELECT   sc.etl_stage_source_id
          , sc.etl_stage_source_code
          , db.etl_stage_source_db_link
          , db.etl_stage_source_db_jdbcname
          , db.etl_stage_source_owner
          , db.etl_stage_distribution_code
          , db.etl_stage_source_bodi_ds
          , db.update_date
       FROM etl_stage_source_db_t db
          , etl_stage_source_t sc
      WHERE sc.etl_stage_source_id = db.etl_stage_source_id
   ORDER BY sc.etl_stage_source_code;

COMMENT ON TABLE etl_stage_source_db_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_source_db_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_source_db_v'
                                 , 'etl_stage_source_db_v'
                                 , TRUE
                                  );
END;
/