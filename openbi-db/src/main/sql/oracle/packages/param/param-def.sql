CREATE OR REPLACE PACKAGE param AUTHID CURRENT_USER AS
  /**
  * $Author: nmarangoni $
  * $Date: $
  * $Revision: $
  * $Id: $
  * $HeadURL: $
  */
  --
  gc_log_off   CONSTANT NUMBER := 0;
  gc_log_fatal CONSTANT NUMBER := 1;
  gc_log_error CONSTANT NUMBER := 2;
  gc_log_warn  CONSTANT NUMBER := 3;
  gc_log_info  CONSTANT NUMBER := 4;
  gc_log_debug constant number := 5;
  gc_log_trace CONSTANT NUMBER := 6;
  gc_log_all   CONSTANT NUMBER := 6;
  --
  g_b_log_console BOOLEAN := FALSE; -- If TRUE, log is sent to dbms_output
  g_b_log_table   BOOLEAN := TRUE; -- If TRUE, log is written in the log table
  g_n_log_level   NUMBER := gc_log_info; -- Log level for the whole session
  --
  g_n_log_message_max NUMBER := 5000; -- maximum number of messages for one session
  g_n_log_console_max NUMBER := 40000; -- maximum Buffer-Size for dbms_out messages
  --
  g_vc_db_name_actual VARCHAR2(100) := sys_context('USERENV'
                                                  ,'DB_NAME');
  g_vc_audsid         VARCHAR2(100) := sys_context('USERENV'
                                                  ,'SESSIONID');
  g_vc_session_user   VARCHAR2(100) := sys_context('USERENV'
                                                  ,'SESSION_USER');
  g_vc_os_user        VARCHAR2(100) := sys_context('USERENV'
                                                  ,'OS_USER');
  g_vc_terminal       VARCHAR2(100) := sys_context('USERENV'
                                                  ,'TERMINAL');
END param;
/