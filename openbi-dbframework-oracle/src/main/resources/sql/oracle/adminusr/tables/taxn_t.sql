BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'taxn'
    , 'taxn_parent_id NUMBER,
	   taxn_order NUMBER,
       taxn_code VARCHAR2 (100),
       taxn_name VARCHAR2 (4000)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;