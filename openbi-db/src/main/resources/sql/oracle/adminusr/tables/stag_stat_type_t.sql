BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'stag_stat_type'
    , 'stag_stat_type_code VARCHAR2(10),
       stag_stat_type_name VARCHAR2(100),
	   stag_stat_type_desc VARCHAR2(1000)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;