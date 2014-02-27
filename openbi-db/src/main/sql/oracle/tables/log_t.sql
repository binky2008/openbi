BEGIN
   aux_ddl.prc_create_entity (
      'log'
    , 'log_severity NUMBER,
       log_message_short VARCHAR2(500 CHAR),
       log_message_long VARCHAR2(4000 CHAR),
       log_object_name VARCHAR2(200 CHAR),
       log_subprogram_name VARCHAR2(200 CHAR),
       log_line_number NUMBER,
	   log_audsid VARCHAR2(100 CHAR),
       log_terminal VARCHAR2(100 CHAR),
       log_rowcount NUMBER,
       log_sqlcode NUMBER,
       log_sqlerrm VARCHAR2(1000 CHAR),
       log_call_stack VARCHAR2(4000 CHAR),
	   log_external_job_id NUMBER'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;
/

COMMENT ON TABLE log_t IS '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';
