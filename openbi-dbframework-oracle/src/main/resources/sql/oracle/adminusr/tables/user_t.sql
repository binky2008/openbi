BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'user'
    , 'user_code VARCHAR2 (10),
       user_name VARCHAR2 (100),
       user_email VARCHAR2 (1000)'
    , 'DROP'
    , TRUE
    , TRUE
    , TRUE
   );
END;