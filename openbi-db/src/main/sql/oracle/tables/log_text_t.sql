SET SERVEROUTPUT ON;

begin
   aux_ddl.prc_create_entity (
      'log_text',
      'log_id NUMBER,
       log_text_big CLOB',
      'DROP',
      TRUE,
      TRUE);
END;
/

COMMENT ON TABLE log_text_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';
