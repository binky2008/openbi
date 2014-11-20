CREATE OR REPLACE VIEW p#frm#stag_source_db_v
AS
     SELECT sc.stag_source_id
          , sc.stag_source_code
          , db.stag_source_db_link
          , db.stag_source_db_jdbcname
          , db.stag_source_owner
          , db.stag_distribution_code
          , db.stag_source_bodi_ds
          , db.update_date
       FROM p#frm#stag_source_db_t db
          , p#frm#stag_source_t sc
      WHERE sc.stag_source_id = db.stag_source_id
   ORDER BY sc.stag_source_code;