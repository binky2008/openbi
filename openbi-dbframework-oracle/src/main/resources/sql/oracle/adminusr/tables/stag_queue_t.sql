BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'stag_queue'
    , 'stag_queue_code VARCHAR2(10),
       stag_queue_name VARCHAR2(100)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;