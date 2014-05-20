BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'mesr_taxn'
    , 'mesr_query_id NUMBER NOT NULL,
	   taxn_id NUMBER NOT NULL'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;