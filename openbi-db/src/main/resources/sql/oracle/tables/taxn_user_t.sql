BEGIN
   ddls.prc_create_entity (
      'taxn_user'
    , 'user_id NUMBER NOT NULL,
       taxn_id NUMBER NOT NULL'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;