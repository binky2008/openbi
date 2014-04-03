BEGIN
   ddls.prc_create_entity (
      'mesr_taxn'
    , 'mesr_query_id NUMBER NOT NULL,
	   taxn_id NUMBER NOT NULL'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;