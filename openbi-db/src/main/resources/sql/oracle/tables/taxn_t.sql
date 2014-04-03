BEGIN
   ddls.prc_create_entity (
      'taxn'
    , 'taxn_parent_id NUMBER,
							taxn_order NUMBER,
                            taxn_code VARCHAR2 (100),
                            taxn_name VARCHAR2 (4000)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;