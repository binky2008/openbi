CREATE OR REPLACE PACKAGE stg_ctl AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-06-08 15:34:39 +0200 (Fr, 08 Jun 2012) $
    * $Revision: 2858 $
    * $Id: stg_ctl-def.sql 2858 2012-06-08 13:34:39Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ctl/stg_ctl-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: stg_ctl-def.sql 2858 2012-06-08 13:34:39Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ctl/stg_ctl-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_queue_ins (
      p_vc_queue_code   VARCHAR2
    , p_vc_queue_name   VARCHAR2
   );

   FUNCTION fct_queue_finished (
      p_n_queue_id   NUMBER
   )
      RETURN BOOLEAN;

   FUNCTION fct_step_available (
      p_n_queue_id   NUMBER
   )
      RETURN BOOLEAN;

   PROCEDURE prc_enqueue_object (
      p_vc_queue_code    VARCHAR2
    , p_vc_source_code   VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name   VARCHAR2 DEFAULT 'ALL'
   );

   PROCEDURE prc_enqueue_source (
      p_vc_source_code         VARCHAR2
    , p_n_threshold_tot_rows   NUMBER
   );

   PROCEDURE prc_execute_step (
      p_n_queue_id   NUMBER
   );

   PROCEDURE prc_execute_queue (
      p_vc_queue_code   VARCHAR2
   );

   PROCEDURE prc_initialize_queue (
      p_vc_queue_code   VARCHAR2
   );

   PROCEDURE prc_truncate_stg1 (
      p_vc_source_code   VARCHAR2
   );

   PROCEDURE prc_bodi_stg1_job_init (
      p_vc_source_code                VARCHAR2
    , p_vc_object_name                VARCHAR2
    , p_stage_id                      NUMBER
    , p_vc_workflow_name              VARCHAR2
    , p_vc_repository_name            VARCHAR2
    , p_n_gui                IN OUT   NUMBER
    , p_n_stat_id            IN OUT   NUMBER
   );

   PROCEDURE prc_bodi_stg1_job_final (
      p_vc_workflow_name     VARCHAR2
    , p_vc_repository_name   VARCHAR2
    , p_n_gui                NUMBER
    , p_n_stat_id            NUMBER
   );

   PROCEDURE prc_bodi_stg1_job_error (
      p_vc_workflow_name     VARCHAR2
    , p_vc_repository_name   VARCHAR2
    , p_n_gui                NUMBER
    , p_n_stat_id            NUMBER
   );
END stg_ctl;
/

SHOW errors