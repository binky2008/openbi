CREATE OR REPLACE PACKAGE BODY aux_log AS
  /**
  * $Author: nmarangoni $
  * $Date: $
  * $Revision: $
  * $Id: $
  * $HeadURL: $
  */

  PROCEDURE purge(p_n_months IN NUMBER DEFAULT 12) IS
  BEGIN
    DELETE aux_log_t
     WHERE create_date < add_months(trunc(SYSDATE)
                                   ,-p_n_months);
    COMMIT;
  END;

  PROCEDURE log_console(p_vc_text IN VARCHAR2) IS
  BEGIN
    IF g_b_message_max_reached THEN
      RETURN;
    END IF;
  
    g_n_console_size := g_n_console_size +
                        length(substr(p_vc_text
                                     ,1
                                     ,255));
  
    IF g_n_console_size >= aux_param.g_n_log_console_max * 0.88 THEN
      -- abzgl. ca. 8% wegen Overhead..
      dbms_output.put_line('--!!Output buffer almost full!!--');
      dbms_output.put_line('--!!No further output in this session!!--');
      dbms_output.put_line('--!!Output truncated!!--');
      g_b_console_max_reached := TRUE;
    ELSE
      dbms_output.put_line(substr(p_vc_text
                                 ,1
                                 ,2000));
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      -- Never Ever Stop working Masterproc
      NULL;
  END;

  PROCEDURE log(p_n_severity        IN NUMBER DEFAULT aux_param.gc_log_info
               ,p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
               ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
               ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
               ,p_vc_text_big       IN CLOB DEFAULT NULL
               ,p_n_row_count       IN NUMBER DEFAULT NULL
               ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
    l_call_stack      VARCHAR2(2000);
    l_call_stack_line VARCHAR2(2000);
    l_tmp_str         VARCHAR2(2000);
    l_line_nr         NUMBER;
    l_sqlcode         NUMBER;
    l_sqlerrm         VARCHAR2(1000);
    l_object_name     VARCHAR2(200);
    l_message_long    VARCHAR2(4000);
    l_message_short   VARCHAR2(500);
    l_log_id          NUMBER;
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF g_b_message_max_reached THEN
      RETURN;
    END IF;
  
    -- Log Only Messages smaller or equal logLevel
    IF p_n_severity <= aux_param.g_n_log_level THEN
      -- counter increment
      g_n_message_count := g_n_message_count + 1;
      l_call_stack      := dbms_utility.format_call_stack;
      l_call_stack_line := substr(l_call_stack
                                 ,instr(l_call_stack
                                       ,chr(10)
                                       ,1
                                       ,4) + 1
                                 ,instr(l_call_stack
                                       ,chr(10)
                                       ,1
                                       ,5) - instr(l_call_stack
                                                  ,chr(10)
                                                  ,1
                                                  ,4));
      l_tmp_str         := TRIM(substr(l_call_stack_line
                                      ,instr(l_call_stack_line
                                            ,' ')));
      l_line_nr         := to_number(substr(l_tmp_str
                                           ,1
                                           ,instr(l_tmp_str
                                                 ,' ') - 1));
      l_object_name     := TRIM(translate(substr(l_tmp_str
                                                ,instr(l_tmp_str
                                                      ,' '))
                                         ,chr(10)
                                         ,' '));
    
      IF g_n_message_count = aux_param.g_n_log_message_max THEN
        l_message_long          := 'Maximum number of messages ' ||
                                   to_char(g_n_message_count) ||
                                   ' for this session reached. No further logging in this session.';
        l_message_short         := l_message_long;
        g_b_message_max_reached := TRUE;
      ELSE
        l_message_long  := substr(p_vc_message_long
                                 ,1
                                 ,4000);
        l_message_short := substr(p_vc_message_short
                                 ,1
                                 ,500);
      END IF;
    
      l_sqlcode := SQLCODE;
      l_sqlerrm := CASE
                     WHEN l_sqlcode <> 0 THEN
                      SQLERRM
                   END;
    
      IF aux_param.g_b_log_console THEN
        log_console('SEVERITY: ' || p_n_severity || '  DATE: ' ||
                    to_char(SYSDATE
                           ,'yyyy-mm-dd hh24:mi:ss'));
        log_console('MESSAGE SHORT: ' || p_vc_message_short);
        log_console('MESSAGE LONG: ' || p_vc_message_long);
        log_console('OBJECT: ' || l_object_name || ' SUBPROGRAM: ' ||
                    p_vc_subprogram || ' LINE: ' || l_line_nr);
        log_console('AUDSID: ' || aux_param.g_vc_audsid ||
                    ' SESSION_USER: ' || aux_param.g_vc_session_user ||
                    ' OS_USER: ' || aux_param.g_vc_os_user ||
                    ' TERMINAL: ' || aux_param.g_vc_terminal);
        log_console('SQLCODE: ' || l_sqlcode || ' SQLERRM: ' || l_sqlerrm);
      END IF;
    
      IF aux_param.g_b_log_table THEN
        INSERT INTO aux_log_t
          (log_severity
          ,log_message_short
          ,log_message_long
          ,log_object_name
          ,log_subprogram_name
          ,log_line_number
          ,log_audsid
          ,log_terminal
          ,log_sqlcode
          ,log_sqlerrm
          ,log_call_stack
          ,log_rowcount
          ,log_external_job_id)
        VALUES
          (p_n_severity
          ,l_message_short
          ,l_message_long
          ,l_object_name
          ,p_vc_subprogram
          ,l_line_nr
          ,aux_param.g_vc_audsid
          ,aux_param.g_vc_terminal
          ,l_sqlcode
          ,l_sqlerrm
          ,CASE
             WHEN p_n_severity < aux_param.gc_log_warn THEN
              l_call_stack
           END
          ,p_n_row_count
          ,p_n_external_job_id)
        RETURNING aux_log_id INTO l_log_id;
      
        COMMIT;
      
        IF p_vc_text_big IS NOT NULL THEN
          INSERT INTO aux_log_text_t
            (aux_log_id
            ,aux_log_text_big)
          VALUES
            (l_log_id
            ,p_vc_text_big);
        
          COMMIT;
        END IF;
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      -- Rollback autonomous transaction
      -- but do not stop working master proc
      ROLLBACK;
  END log;

  /**
  * Simplified log procedures
  */

  PROCEDURE log_fatal(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_fatal
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_error(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_error
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
  END;

  PROCEDURE log_warn(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                    ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                    ,p_vc_text_big       IN CLOB DEFAULT NULL
                    ,p_n_row_count       IN NUMBER DEFAULT NULL
                    ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_warn
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
  END;

  PROCEDURE log_info(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                    ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                    ,p_vc_text_big       IN CLOB DEFAULT NULL
                    ,p_n_row_count       IN NUMBER DEFAULT NULL
                    ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_info
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
  END;

  PROCEDURE log_debug(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_debug
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
  END;

  PROCEDURE log_trace(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_trace
       ,NULL
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
  END;

  /**
  * Simplified log procedures for non-standalone PL/SQL subprograms (procedures and functions)
  */

  PROCEDURE log_sub_fatal(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_fatal
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_sub_error(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_error
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_sub_warn(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                        ,p_vc_text_big       IN CLOB DEFAULT NULL
                        ,p_n_row_count       IN NUMBER DEFAULT NULL
                        ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_warn
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_sub_info(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                        ,p_vc_text_big       IN CLOB DEFAULT NULL
                        ,p_n_row_count       IN NUMBER DEFAULT NULL
                        ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_info
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_sub_debug(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_debug
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE log_sub_trace(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL) IS
  BEGIN
  
    log(aux_param.gc_log_trace
       ,p_vc_subprogram
       ,p_vc_message_short
       ,p_vc_message_long
       ,p_vc_text_big
       ,p_n_row_count
       ,p_n_external_job_id);
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

/**
  * Package initialization
  */
BEGIN
  -- set package variables
  g_n_message_count       := 0; -- Initialize Message Counter
  g_b_message_max_reached := FALSE;
  --
  g_n_console_size        := 0;
  g_b_console_max_reached := FALSE;

  IF aux_param.g_b_log_console THEN
    dbms_output.enable(aux_param.g_n_log_console_max);
  END IF;

  --
  c_body_version := '$Id: $';
  c_body_url     := '$HeadURL: $';
EXCEPTION
  WHEN OTHERS THEN
    -- Never ever stop working master procedure
    NULL;
END aux_log;
/
