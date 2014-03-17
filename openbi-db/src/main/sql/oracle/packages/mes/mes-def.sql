CREATE OR REPLACE PACKAGE mes AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-01-16 14:40:01 +0100 (Mo, 16 Jan 2012) $
    * $Revision: 2176 $
    * $Id: pkg_qc-def.sql 2176 2012-01-16 13:40:01Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_qc/pkg_qc-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_qc-def.sql 2176 2012-01-16 13:40:01Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_qc/pkg_qc-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_case_taxonomy_ins (
      p_vc_case_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   );

   PROCEDURE prc_case_taxonomy_del (
      p_vc_case_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   );

   PROCEDURE prc_case_ins (
      p_vc_case_code          IN   VARCHAR2
    , p_vc_case_name          IN   VARCHAR2
    , p_vc_layer_code         IN   VARCHAR2 DEFAULT 'GLOBAL'
    , p_vc_entity_code        IN   VARCHAR2 DEFAULT 'GLOBAL'
    , p_vc_environment_code   IN   VARCHAR2 DEFAULT 'GLOBAL'
   );

   PROCEDURE prc_case_del (
      p_vc_case_code   IN   VARCHAR2
    , p_b_cascade      IN   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_step_ins (
      p_vc_case_code   IN   VARCHAR2
    , p_n_step_order   IN   NUMBER
    , p_vc_step_code   IN   VARCHAR2
    , p_vc_step_name   IN   VARCHAR2
    , p_vc_step_sql    IN   CLOB
   );

   PROCEDURE prc_step_del (
      p_vc_case_code   IN   VARCHAR2
    , p_vc_step_code   IN   VARCHAR2
    , p_b_cascade      IN   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_keyfigure_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_keyfigure_name   IN   VARCHAR2
   );

   PROCEDURE prc_keyfigure_del (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_b_cascade           IN   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_threshold_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_threshold_type   IN   VARCHAR2
    , p_n_threshold_min     IN   NUMBER
    , p_n_threshold_max     IN   NUMBER
    , p_d_threshold_from    IN   DATE DEFAULT TO_DATE ('01011111', 'ddmmyyyy')
    , p_d_threshold_to      IN   DATE DEFAULT TO_DATE ('09099999', 'ddmmyyyy')
   );

   PROCEDURE prc_exec_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_n_result_value      IN   NUMBER
    , p_vc_result_report    IN   CLOB
   );

   PROCEDURE prc_exec (
      p_vc_case_code           IN   VARCHAR2 DEFAULT 'ALL'
    , p_vc_step_code           IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   );

   PROCEDURE prc_exec_taxonomy (
      p_vc_taxonomy_code       IN   VARCHAR2
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   );
END mes;
/

SHOW errors