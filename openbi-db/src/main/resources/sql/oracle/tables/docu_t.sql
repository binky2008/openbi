BEGIN
   ddls.prc_create_entity (
      'docu'
    , 'docu_type VARCHAR2 (100),
       docu_code VARCHAR2 (100),
       docu_url VARCHAR2 (4000),
       docu_desc VARCHAR2 (4000),
       docu_content CLOB'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;
/