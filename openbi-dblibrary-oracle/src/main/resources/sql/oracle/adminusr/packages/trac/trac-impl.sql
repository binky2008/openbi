CREATE OR REPLACE PACKAGE BODY p#frm#trac
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: trac-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/trac/trac-impl.sql $
   */
   PROCEDURE purge (p_n_months IN NUMBER DEFAULT 12)
   IS
   BEGIN
      DELETE p#frm#trac_t
       WHERE create_date < ADD_MONTHS (
                              TRUNC (SYSDATE)
                            , -p_n_months
                           );

      COMMIT;
   END;

   PROCEDURE log_console (p_vc_text IN VARCHAR2)
   IS
   BEGIN
      IF g_b_message_max_reached THEN
         RETURN;
      END IF;

      g_n_console_size :=
           g_n_console_size
         + LENGTH (SUBSTR (
                      p_vc_text
                    , 1
                    , 255
                   ));

      IF g_n_console_size >= p#frm#trac_param.g_n_log_console_max * 0.88 THEN
         -- abzgl. ca. 8% wegen Overhead..
         DBMS_OUTPUT.put_line ('--!!Output buffer almost full!!--');
         DBMS_OUTPUT.put_line ('--!!No further output in this session!!--');
         DBMS_OUTPUT.put_line ('--!!Output truncated!!--');
         g_b_console_max_reached := TRUE;
      ELSE
         DBMS_OUTPUT.put_line (SUBSTR (
                                  p_vc_text
                                , 1
                                , 2000
                               ));
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         -- Never Ever Stop working Masterproc
         NULL;
   END;

   PROCEDURE LOG (
      p_n_severity          IN NUMBER DEFAULT p#frm#trac_param.gc_log_info
    , p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
    , p_vc_call_stack       IN VARCHAR2 DEFAULT NULL
   )
   IS
      l_call_stack        VARCHAR2 (2000);
      l_call_stack_line   VARCHAR2 (2000);
      l_tmp_str           VARCHAR2 (2000);
      l_line_nr           NUMBER;
      l_sqlcode           NUMBER;
      l_sqlerrm           VARCHAR2 (1000);
      l_object_name       VARCHAR2 (200);
      l_message_long      VARCHAR2 (4000);
      l_message_short     VARCHAR2 (500);
      l_trac_id           NUMBER;
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      IF g_b_message_max_reached THEN
         RETURN;
      END IF;

      -- Log Only Messages smaller or equal logLevel
      IF p_n_severity <= p#frm#trac_param.g_n_log_level THEN
         -- counter increment
         g_n_message_count :=
              g_n_message_count
            + 1;
         l_call_stack := NVL (p_vc_call_stack, DBMS_UTILITY.format_call_stack);
         l_call_stack_line :=
            SUBSTR (
               l_call_stack
             ,   INSTR (
                    l_call_stack
                  , CHR (10)
                  , 1
                  , 4
                 )
               + 1
             ,   INSTR (
                    l_call_stack
                  , CHR (10)
                  , 1
                  , 5
                 )
               - INSTR (
                    l_call_stack
                  , CHR (10)
                  , 1
                  , 4
                 )
            );
         l_tmp_str :=
            TRIM (SUBSTR (
                     l_call_stack_line
                   , INSTR (
                        l_call_stack_line
                      , ' '
                     )
                  ));
         l_line_nr :=
            TO_NUMBER (SUBSTR (
                          l_tmp_str
                        , 1
                        ,   INSTR (
                               l_tmp_str
                             , ' '
                            )
                          - 1
                       ));
         l_object_name :=
            TRIM (TRANSLATE (
                     SUBSTR (
                        l_tmp_str
                      , INSTR (
                           l_tmp_str
                         , ' '
                        )
                     )
                   , CHR (10)
                   , ' '
                  ));

         IF g_n_message_count = p#frm#trac_param.g_n_log_message_max THEN
            l_message_long :=
                  'Maximum number of messages '
               || TO_CHAR (g_n_message_count)
               || ' for this session reached. No further logging in this session.';
            l_message_short := l_message_long;
            g_b_message_max_reached := TRUE;
         ELSE
            l_message_long :=
               SUBSTR (
                  p_vc_message_long
                , 1
                , 4000
               );
            l_message_short :=
               SUBSTR (
                  p_vc_message_short
                , 1
                , 500
               );
         END IF;

         l_sqlcode := SQLCODE;
         l_sqlerrm :=
            CASE
               WHEN l_sqlcode <> 0 THEN
                  SQLERRM
            END;

         IF p#frm#trac_param.g_b_log_console THEN
            log_console (   'SEVERITY: '
                         || p_n_severity
                         || '  DATE: '
                         || TO_CHAR (
                               SYSDATE
                             , 'yyyy-mm-dd hh24:mi:ss'
                            ));
            log_console (   'MESSAGE SHORT: '
                         || p_vc_message_short);
            log_console (   'MESSAGE LONG: '
                         || p_vc_message_long);
            log_console (   'OBJECT: '
                         || l_object_name
                         || ' SUBPROGRAM: '
                         || p_vc_subprogram
                         || ' LINE: '
                         || l_line_nr);
            log_console (   'AUDSID: '
                         || p#frm#trac_param.g_vc_audsid
                         || ' SESSION_USER: '
                         || p#frm#trac_param.g_vc_session_user
                         || ' OS_USER: '
                         || p#frm#trac_param.g_vc_os_user
                         || ' TERMINAL: '
                         || p#frm#trac_param.g_vc_terminal);
            log_console (   'SQLCODE: '
                         || l_sqlcode
                         || ' SQLERRM: '
                         || l_sqlerrm);
         END IF;

         IF p#frm#trac_param.g_b_log_table THEN
            INSERT INTO p#frm#trac_t (
                           trac_severity
                         , trac_message_short
                         , trac_message_long
                         , trac_object_name
                         , trac_subprogram_name
                         , trac_line_number
                         , trac_audsid
                         , trac_terminal
                         , trac_sqlcode
                         , trac_sqlerrm
                         , trac_call_stack
                         , trac_rowcount
                         , trac_external_job_id
                         , trac_text
                        )
                 VALUES (
                           p_n_severity
                         , l_message_short
                         , l_message_long
                         , l_object_name
                         , p_vc_subprogram
                         , l_line_nr
                         , p#frm#trac_param.g_vc_audsid
                         , p#frm#trac_param.g_vc_terminal
                         , l_sqlcode
                         , l_sqlerrm
                         , CASE
                              WHEN p_n_severity < p#frm#trac_param.gc_log_warn THEN
                                 l_call_stack
                           END
                         , p_n_row_count
                         , p_n_external_job_id
                         , p_vc_text_big
                        )
              RETURNING trac_id
                   INTO l_trac_id;

            COMMIT;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         -- Rollback autonomous transaction
         -- but do not stop working master proc
         ROLLBACK;
   END LOG;

   /**
   * Simplified log procedures
   */
   PROCEDURE log_fatal (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_fatal
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_error (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_error
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_warn (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_warn
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_info (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_info
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_debug (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_debug
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_trace (
      p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_trace
       , NULL
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   /**
   * Simplified log procedures for non-standalone PL/SQL subprograms (procedures and functions)
   */
   PROCEDURE log_sub_fatal (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_fatal
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_sub_error (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_error
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_sub_warn (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_warn
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_sub_info (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_info
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_sub_debug (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_debug
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;

   PROCEDURE log_sub_trace (
      p_vc_subprogram       IN VARCHAR2 DEFAULT NULL
    , p_vc_message_short    IN VARCHAR2 DEFAULT NULL
    , p_vc_message_long     IN VARCHAR2 DEFAULT NULL
    , p_vc_text_big         IN CLOB DEFAULT NULL
    , p_n_row_count         IN NUMBER DEFAULT NULL
    , p_n_external_job_id   IN NUMBER DEFAULT NULL
   )
   IS
   BEGIN
      LOG (
         p#frm#trac_param.gc_log_trace
       , p_vc_subprogram
       , p_vc_message_short
       , p_vc_message_long
       , p_vc_text_big
       , p_n_row_count
       , p_n_external_job_id
       , DBMS_UTILITY.format_call_stack
      );
   EXCEPTION
      WHEN OTHERS THEN
         ROLLBACK;
   END;
/**
  * Package initialization
  */
BEGIN
   -- set package variables
   g_n_message_count := 0;                                                                                                                                                                                                        -- Initialize Message Counter
   g_b_message_max_reached := FALSE;
   --
   g_n_console_size := 0;
   g_b_console_max_reached := FALSE;

   IF p#frm#trac_param.g_b_log_console THEN
      DBMS_OUTPUT.enable (p#frm#trac_param.g_n_log_console_max);
   END IF;

   --
   c_body_version := '$Id: trac-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/trac/trac-impl.sql $';
EXCEPTION
   WHEN OTHERS THEN
      -- Never ever stop working master procedure
      NULL;
END p#frm#trac;