BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'trac'
    , 'trac_severity NUMBER,
       trac_message_short VARCHAR2(500 CHAR),
       trac_message_long VARCHAR2(4000 CHAR),
       trac_text CLOB,
       trac_object_name VARCHAR2(200 CHAR),
       trac_subprogram_name VARCHAR2(200 CHAR),
       trac_line_number NUMBER,
	   trac_audsid VARCHAR2(100 CHAR),
       trac_terminal VARCHAR2(100 CHAR),
       trac_rowcount NUMBER,
       trac_sqlcode NUMBER,
       trac_sqlerrm VARCHAR2(1000 CHAR),
       trac_call_stack VARCHAR2(4000 CHAR),
	   trac_external_job_id NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;