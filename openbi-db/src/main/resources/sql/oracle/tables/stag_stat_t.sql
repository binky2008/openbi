BEGIN
   ddls.prc_create_entity (
      'stag_stat'
    , 'stag_id NUMBER,
	   stag_object_id NUMBER,
	   stag_partition NUMBER,
	   stag_load_id NUMBER,
	   stag_stat_type_id NUMBER,
	   stag_stat_value NUMBER,
	   stag_stat_error NUMBER,
	   stag_stat_sid NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;