BEGIN
   ddl.prc_create_entity (
      'trc'
    , 'trc_severity NUMBER,
       trc_message_short VARCHAR2(500 CHAR),
       trc_message_long VARCHAR2(4000 CHAR),
       trc_text CLOB,
       trc_object_name VARCHAR2(200 CHAR),
       trc_subprogram_name VARCHAR2(200 CHAR),
       trc_line_number NUMBER,
	   trc_audsid VARCHAR2(100 CHAR),
       trc_terminal VARCHAR2(100 CHAR),
       trc_rowcount NUMBER,
       trc_sqlcode NUMBER,
       trc_sqlerrm VARCHAR2(1000 CHAR),
       trc_call_stack VARCHAR2(4000 CHAR),
	   trc_external_job_id NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;
/

COMMENT ON TABLE trc_t IS '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';
