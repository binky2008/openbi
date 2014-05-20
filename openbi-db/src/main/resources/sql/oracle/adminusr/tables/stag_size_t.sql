BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'stag_size'
    , 'stag_object_id NUMBER,
       stag_table_name VARCHAR2(100),
       stag_num_rows NUMBER,
       stag_bytes NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;