BEGIN
   ddls.prc_create_entity (
      'stag_ddl'
    , 'stag_ddl_type VARCHAR2 (100),
       stag_ddl_name VARCHAR2 (100),
       stag_ddl_code CLOB'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;