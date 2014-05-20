BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'stag_source_db'
    , 'stag_source_id          NUMBER,
       stag_source_db_link     VARCHAR2(100),
       stag_source_db_jdbcname VARCHAR2(100),
       stag_source_owner       VARCHAR2(100),
       stag_distribution_code  VARCHAR2(10),
       stag_source_bodi_ds     VARCHAR2(100)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;