CREATE OR REPLACE PACKAGE BODY pkg_enable
AS
   /**
   * $Author: nmarangoni $
   * $Date: 2012-05-15 11:00:31 +0200 (Di, 15 Mai 2012) $
   * $Revision: 2788 $
   * $Id: pkg_enable-impl.sql 2788 2012-05-15 09:00:31Z nmarangoni $
   * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-impl.sql $
   */
   TYPE t_statement IS TABLE OF VARCHAR2 (1000);

   l_grant_utl      t_statement
      := t_statement ('GRANT INSERT,UPDATE ON sys_entity_t TO '
                    , 'GRANT INSERT,UPDATE ON sys_environment_t TO '
                    , 'GRANT INSERT,UPDATE ON sys_layer_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON sys_taxonomy_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON sys_user_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON sys_user_taxonomy_t TO '
                    , 'GRANT INSERT ON utl_log_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_load_statistics_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_parameter_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_job_status_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON utl_doc_t TO '
                    , 'GRANT EXECUTE ON pkg_param TO '
                    , 'GRANT EXECUTE ON pkg_sys TO '
                    , 'GRANT EXECUTE ON pkg_utl_log TO '
                    , 'GRANT EXECUTE ON pkg_utl_job TO '
                    , 'GRANT EXECUTE ON pkg_utl_ddl TO '
                    , 'GRANT EXECUTE ON pkg_utl_doc_template TO '
                    , 'GRANT EXECUTE ON pkg_utl_doc TO '
                    , 'GRANT EXECUTE ON pkg_utl_type TO '
                    , 'GRANT EXECUTE ON pkg_utl_parameter TO '
                    , 'GRANT EXECUTE ON pkg_utl_hash TO '
                     );
   l_revoke_utl     t_statement
      := t_statement ('REVOKE INSERT,UPDATE ON sys_entity_t FROM '
                    , 'REVOKE INSERT,UPDATE ON sys_environment_t FROM '
                    , 'REVOKE INSERT,UPDATE ON sys_layer_t FROM '
                    , 'REVOKE INSERT,UPDATE ON sys_taxonomy_t FROM '
                    , 'REVOKE INSERT,UPDATE ON sys_user_t FROM '
                    , 'REVOKE INSERT,UPDATE ON sys_user_taxonomy_t FROM '
                    , 'REVOKE INSERT ON utl_log_t FROM '
                    , 'REVOKE INSERT,UPDATE ON utl_load_statistics_t FROM '
                    , 'REVOKE INSERT,UPDATE ON utl_parameter_t FROM '
                    , 'REVOKE INSERT,UPDATE ON utl_job_status_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON utl_doc_t FROM '
                    , 'REVOKE EXECUTE ON pkg_param FROM '
                    , 'REVOKE EXECUTE ON pkg_sys FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_log FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_job FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_ddl FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_doc_template FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_doc FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_type FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_parameter FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_hash FROM '
                     );
   l_grant_qc       t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON qc_case_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_case_taxonomy_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_step_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_keyfigure_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_threshold_t TO '
                    , 'GRANT INSERT,DELETE ON qc_exec_t TO '
                    , 'GRANT EXECUTE ON pkg_qc TO '
                    , 'GRANT EXECUTE ON pkg_qc_stage TO '
                    , 'GRANT EXECUTE ON pkg_qc_core TO '
                     );
   l_revoke_qc      t_statement
      := t_statement ('REVOKE INSERT,UPDATE,DELETE ON qc_case_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON qc_case_taxonomy_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON qc_step_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON qc_keyfigure_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON qc_threshold_t FROM '
                    , 'REVOKE INSERT,DELETE ON qc_exec_t FROM '
                    , 'REVOKE EXECUTE ON pkg_qc FROM '
                    , 'REVOKE EXECUTE ON pkg_qc_stage FROM '
                    , 'REVOKE EXECUTE ON pkg_qc_core FROM '
                     );
   l_grant_stage    t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON etl_stage_stat_type_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_stat_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_size_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_column_tmp TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_source_db_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_column_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_column_check_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_source_t tO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_object_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_ddl_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_queue_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_queue_object_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON etl_stage_curr_hist_t TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_param TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_stat TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_meta TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_ddl TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_build TO '
                    , 'GRANT EXECUTE ON pkg_etl_stage_ctl TO '
                     );
   l_revoke_stage   t_statement
      := t_statement ('REVOKE INSERT,UPDATE,DELETE ON etl_stage_stat_type_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_stat_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_size_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_column_tmp FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_source_db_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_column_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_column_check_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_source_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_object_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_ddl_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_queue_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_queue_object_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON etl_stage_curr_hist_t FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_param FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_stat FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_meta FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_ddl FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_build FROM '
                    , 'REVOKE EXECUTE ON pkg_etl_stage_ctl FROM '
                     );
   l_grant_core     t_statement := t_statement ('GRANT EXECUTE ON pkg_etl_framework TO ', 'GRANT EXECUTE ON pkg_lkp_d_day TO ');
   l_revoke_core    t_statement := t_statement ('REVOKE EXECUTE ON pkg_etl_framework FROM ', 'REVOKE EXECUTE ON pkg_lkp_d_day FROM ');

   PROCEDURE prc_enable_utl (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_utl.FIRST .. l_grant_utl.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_utl (i) || p_vc_schema;
      END LOOP;
   END prc_enable_utl;

   PROCEDURE prc_disable_utl (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_utl.FIRST .. l_revoke_utl.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_utl (i) || p_vc_schema;
      END LOOP;
   END prc_disable_utl;

   PROCEDURE prc_enable_qc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_qc.FIRST .. l_grant_qc.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_qc (i) || p_vc_schema;
      END LOOP;
   END prc_enable_qc;

   PROCEDURE prc_disable_qc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_qc.FIRST .. l_revoke_qc.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_qc (i) || p_vc_schema;
      END LOOP;
   END prc_disable_qc;

   PROCEDURE prc_enable_stage (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_stage.FIRST .. l_grant_stage.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_stage (i) || p_vc_schema;
      END LOOP;
   END prc_enable_stage;

   PROCEDURE prc_disable_stage (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_stage.FIRST .. l_revoke_stage.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_stage (i) || p_vc_schema;
      END LOOP;
   END prc_disable_stage;

   PROCEDURE prc_enable_core (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_core.FIRST .. l_grant_core.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_core (i) || p_vc_schema;
      END LOOP;
   END prc_enable_core;

   PROCEDURE prc_disable_core (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_core.FIRST .. l_revoke_core.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_core (i) || p_vc_schema;
      END LOOP;
   END prc_disable_core;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_enable-impl.sql 2788 2012-05-15 09:00:31Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-impl.sql $';
END pkg_enable;
/

SHOW errors