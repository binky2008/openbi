CREATE OR REPLACE PACKAGE stag_ctl
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_queue_ins (
      p_vc_queue_code    VARCHAR2
    , p_vc_queue_name    VARCHAR2
   );

   FUNCTION fct_queue_finished (p_n_queue_id NUMBER)
      RETURN BOOLEAN;

   FUNCTION fct_step_available (p_n_queue_id NUMBER)
      RETURN BOOLEAN;

   PROCEDURE prc_enqueue_object (
      p_vc_queue_code     VARCHAR2
    , p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
   );

   PROCEDURE prc_enqueue_source (
      p_vc_source_code          VARCHAR2
    , p_n_threshold_tot_rows    NUMBER
   );

   PROCEDURE prc_execute_step (p_n_queue_id NUMBER);

   PROCEDURE prc_execute_queue (p_vc_queue_code VARCHAR2);

   PROCEDURE prc_initialize_queue (p_vc_queue_code VARCHAR2);

   PROCEDURE prc_truncate_stage (p_vc_source_code VARCHAR2);
END stag_ctl;