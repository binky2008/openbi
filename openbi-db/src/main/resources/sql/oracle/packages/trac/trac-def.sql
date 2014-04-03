CREATE OR REPLACE PACKAGE trac AS
  /**
  * APIs for Logging and Tracing in the table log_t on through DBMS_OUTPUT.
  *
  * $Author: nmarangoni $
  * $Date: $
  * $Revision: $
  * $Id: $
  * $HeadURL: $
  */

  /**
  * Package Spec Version String.
  */
  c_spec_version CONSTANT VARCHAR2(400) := '$Id: $';
  /**
  * Package spec repository URL.
  */
  c_spec_url CONSTANT VARCHAR2(1024) := '$HeadURL: $';
  /**
  * Package Body Version String.
  */
  c_body_version VARCHAR2(400);
  /**
  * Package body repository URL.
  */
  c_body_url VARCHAR2(1024);
  /**
  * Global log constants
  * This constants are used to measure the size of the generated logs
  */
  g_n_message_count       NUMBER; -- counter for messages of this session
  g_b_message_max_reached BOOLEAN; -- has the max number of messages already been reached
  --
  g_n_console_size        NUMBER; -- current Buffer-Size in Bytes
  g_b_console_max_reached BOOLEAN; -- has the dbms_out limit been reached?

  /**
  * Log a message to DBMS_OUTPUT and/or in the table log_t
  *
  * @param          p_n_months    months to preserve
  */
  PROCEDURE purge(p_n_months IN NUMBER DEFAULT 12);

  /**
  * Log a message to DBMS_OUTPUT and/or in the table log_t
  *
  * @param          p_n_severity           severity (from fatal to debug)
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */
  PROCEDURE log(p_n_severity        IN NUMBER DEFAULT trac_param.gc_log_info
               ,p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
               ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
               ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
               ,p_vc_text_big       IN CLOB DEFAULT NULL
               ,p_n_row_count       IN NUMBER DEFAULT NULL
               ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Simplified log procedures
  */

  /**
  * Log a fatal entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_fatal(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a error entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_error(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a warning entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_warn(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                    ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                    ,p_vc_text_big       IN CLOB DEFAULT NULL
                    ,p_n_row_count       IN NUMBER DEFAULT NULL
                    ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a informational entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          Count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_info(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                    ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                    ,p_vc_text_big       IN CLOB DEFAULT NULL
                    ,p_n_row_count       IN NUMBER DEFAULT NULL
                    ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a debug entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_debug(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a trace entry
  *
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_trace(p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                     ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                     ,p_vc_text_big       IN CLOB DEFAULT NULL
                     ,p_n_row_count       IN NUMBER DEFAULT NULL
                     ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Simplified log procedures for non-standalone PL/SQL subprograms (procedures and functions)
  */

  /**
  * Log a fatal entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_fatal(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a error entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_error(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a warning entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_warn(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                        ,p_vc_text_big       IN CLOB DEFAULT NULL
                        ,p_n_row_count       IN NUMBER DEFAULT NULL
                        ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a informational entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_info(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                        ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                        ,p_vc_text_big       IN CLOB DEFAULT NULL
                        ,p_n_row_count       IN NUMBER DEFAULT NULL
                        ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a debug entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_debug(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL);

  /**
  * Log a trace entry
  *
  * @param          p_vc_subprogram        subprogram (procedure name)
  * @param          p_vc_message_short     short message text
  * @param          p_vc_message_long      long message text
  * @param          p_vc_text_big          big text
  * @param          p_n_row_count          count of worked rows
  * @param          p_n_external_job_id    CLOB text (for storing big generated texts or object-ddls)
  */

  PROCEDURE log_sub_trace(p_vc_subprogram     IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_short  IN VARCHAR2 DEFAULT NULL
                         ,p_vc_message_long   IN VARCHAR2 DEFAULT NULL
                         ,p_vc_text_big       IN CLOB DEFAULT NULL
                         ,p_n_row_count       IN NUMBER DEFAULT NULL
                         ,p_n_external_job_id IN NUMBER DEFAULT NULL);

END trac;