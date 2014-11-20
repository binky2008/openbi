BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'mesr_query'
    , 'mesr_query_code VARCHAR2(100) NOT NULL,
	   mesr_query_name VARCHAR2(1000),
	   mesr_query_sql CLOB'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;