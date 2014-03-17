CREATE OR REPLACE PACKAGE BODY enable
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
      := t_statement ('GRANT EXECUTE ON param TO '
                    , 'GRANT EXECUTE ON type TO '
                    , 'GRANT EXECUTE ON dict TO '
                    , 'GRANT EXECUTE ON ddl TO '
                     );
   l_revoke_utl     t_statement
      := t_statement ('REVOKE EXECUTE ON param FROM '
                    , 'REVOKE EXECUTE ON type FROM '
                    , 'REVOKE EXECUTE ON dict FROM '
                    , 'REVOKE EXECUTE ON ddl FROM '
                     );

   l_grant_trc      t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON sys_user_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON sys_taxonomy_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON sys_user_taxonomy_t TO '
                    , 'GRANT INSERT ON utl_log_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_load_statistics_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_parameter_t TO '
                    , 'GRANT INSERT,UPDATE ON utl_job_status_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON utl_doc_t TO '
                    , 'GRANT EXECUTE ON pkg_param TO '
                    , 'GRANT EXECUTE ON pkg_sys TO '
                    , 'GRANT EXECUTE ON log TO '
                    , 'GRANT EXECUTE ON pkg_utl_job TO '
                    , 'GRANT EXECUTE ON ddl TO '
                    , 'GRANT EXECUTE ON pkg_utl_doc_template TO '
                    , 'GRANT EXECUTE ON pkg_utl_doc TO '
                    , 'GRANT EXECUTE ON pkg_utl_type TO '
                    , 'GRANT EXECUTE ON pkg_utl_parameter TO '
                    , 'GRANT EXECUTE ON pkg_utl_hash TO '
                     );
   l_revoke_trc     t_statement
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
                    , 'REVOKE EXECUTE ON log FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_job FROM '
                    , 'REVOKE EXECUTE ON ddl FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_doc_template FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_doc FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_type FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_parameter FROM '
                    , 'REVOKE EXECUTE ON pkg_utl_hash FROM '
                     );
   l_grant_mes       t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON user_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON txn_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON user_txn_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_case_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_case_taxonomy_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_step_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_keyfigure_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON qc_threshold_t TO '
                    , 'GRANT INSERT,DELETE ON qc_exec_t TO '
                    , 'GRANT EXECUTE ON pkg_qc TO '
                    , 'GRANT EXECUTE ON pkg_qc_stage TO '
                    , 'GRANT EXECUTE ON pkg_qc_core TO '
                     );
   l_revoke_mes      t_statement
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
   l_grant_stg    t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON stg_stat_type_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_stat_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_size_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_tmp TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_source_db_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_check_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_source_t tO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_object_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_ddl_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_queue_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_queue_object_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_curr_hist_t TO '
                    , 'GRANT EXECUTE ON stg_param TO '
                    , 'GRANT EXECUTE ON stg_stat TO '
                    , 'GRANT EXECUTE ON stg_meta TO '
                    , 'GRANT EXECUTE ON stg_ddl TO '
                    , 'GRANT EXECUTE ON stg_build TO '
                    , 'GRANT EXECUTE ON stg_ctl TO '
                     );
   l_revoke_stg   t_statement
      := t_statement ('REVOKE INSERT,UPDATE,DELETE ON stg_stat_type_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_stat_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_size_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_tmp FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_source_db_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_check_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_source_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_object_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_ddl_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_queue_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_queue_object_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_curr_hist_t FROM '
                    , 'REVOKE EXECUTE ON stg_param FROM '
                    , 'REVOKE EXECUTE ON stg_stat FROM '
                    , 'REVOKE EXECUTE ON stg_meta FROM '
                    , 'REVOKE EXECUTE ON stg_ddl FROM '
                    , 'REVOKE EXECUTE ON stg_build FROM '
                    , 'REVOKE EXECUTE ON stg_ctl FROM '
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

   PROCEDURE prc_enable_trc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_trc.FIRST .. l_grant_trc.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_trc (i) || p_vc_schema;
      END LOOP;
   END prc_enable_trc;

   PROCEDURE prc_disable_trc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_trc.FIRST .. l_revoke_trc.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_trc (i) || p_vc_schema;
      END LOOP;
   END prc_disable_trc;

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

   PROCEDURE prc_enable_mes (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_mes.FIRST .. l_grant_mes.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_mes (i) || p_vc_schema;
      END LOOP;
   END prc_enable_mes;

   PROCEDURE prc_disable_mes (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_mes.FIRST .. l_revoke_mes.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_mes (i) || p_vc_schema;
      END LOOP;
   END prc_disable_mes;

   PROCEDURE prc_enable_stg (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_stg.FIRST .. l_grant_stg.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_stg (i) || p_vc_schema;
      END LOOP;
   END prc_enable_stg;

   PROCEDURE prc_disable_stg (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_stg.FIRST .. l_revoke_stg.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_stg (i) || p_vc_schema;
      END LOOP;
   END prc_disable_stg;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_enable-impl.sql 2788 2012-05-15 09:00:31Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-impl.sql $';
END enable;
/

SHOW errors