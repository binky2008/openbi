BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'mesr_keyfigure'
    , 'mesr_query_id NUMBER,
       mesr_keyfigure_code VARCHAR2(100) NOT NULL,
       mesr_keyfigure_name VARCHAR2(1000)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;