BEGIN
   ddls.prc_create_entity (
      'stag_column_check'
    , 'stag_object_id NUMBER,
	   stag_column_name VARCHAR2 (100),
	   stag_column_pos NUMBER,
	   stag_column_def VARCHAR2 (100),
	   stag_column_nk_pos NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;