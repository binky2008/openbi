CREATE OR REPLACE PACKAGE BODY authorize
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
      := t_statement ('GRANT INSERT,UPDATE ON trc_t TO '
                    , 'GRANT EXECUTE ON trc TO '
                     );
   l_revoke_trc     t_statement
      := t_statement ('REVOKE INSERT,UPDATE ON trc_t FROM '
                    , 'REVOKE EXECUTE ON trc FROM '
                     );
   l_grant_mes       t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON user_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON txn_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON txn_user_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON mes_txn_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON mes_query_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON mes_keyfigure_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON mes_threshold_t TO '
                    , 'GRANT INSERT,DELETE ON mes_exec_t TO '
                    , 'GRANT EXECUTE ON mes TO '
                     );
   l_revoke_mes      t_statement
      := t_statement ('REVOKE INSERT,UPDATE,DELETE ON user_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON txn_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON txn_user_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON mes_txn_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON mes_query_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON mes_keyfigure_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON mes_threshold_t FROM '
                    , 'REVOKE INSERT,DELETE ON mes_exec_t FROM '
                    , 'REVOKE EXECUTE ON mes FROM '
                     );
   l_grant_stg    t_statement
      := t_statement ('GRANT INSERT,UPDATE,DELETE ON stg_stat_type_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_stat_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_size_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_ddl_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_tmp TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_source_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_source_db_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_object_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_column_check_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_queue_t TO '
                    , 'GRANT INSERT,UPDATE,DELETE ON stg_queue_object_t TO '
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
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_ddl_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_tmp FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_source_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_source_db_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_object_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_column_check_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_queue_t FROM '
                    , 'REVOKE INSERT,UPDATE,DELETE ON stg_queue_object_t FROM '
                    , 'REVOKE EXECUTE ON stg_param FROM '
                    , 'REVOKE EXECUTE ON stg_stat FROM '
                    , 'REVOKE EXECUTE ON stg_meta FROM '
                    , 'REVOKE EXECUTE ON stg_ddl FROM '
                    , 'REVOKE EXECUTE ON stg_build FROM '
                    , 'REVOKE EXECUTE ON stg_ctl FROM '
                     );
   l_grant_core     t_statement := t_statement ('GRANT EXECUTE ON pkg_etl_framework TO ', 'GRANT EXECUTE ON pkg_lkp_d_day TO ');
   l_revoke_core    t_statement := t_statement ('REVOKE EXECUTE ON pkg_etl_framework FROM ', 'REVOKE EXECUTE ON pkg_lkp_d_day FROM ');

   PROCEDURE prc_grant_utl (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_utl.FIRST .. l_grant_utl.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_utl (i) || p_vc_schema;
      END LOOP;
   END prc_grant_utl;

   PROCEDURE prc_grant_trc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_trc.FIRST .. l_grant_trc.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_trc (i) || p_vc_schema;
      END LOOP;
   END prc_grant_trc;

   PROCEDURE prc_revoke_trc (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_trc.FIRST .. l_revoke_trc.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_trc (i) || p_vc_schema;
      END LOOP;
   END prc_revoke_trc;

   PROCEDURE prc_revoke_utl (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_utl.FIRST .. l_revoke_utl.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_utl (i) || p_vc_schema;
      END LOOP;
   END prc_revoke_utl;

   PROCEDURE prc_grant_mes (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_mes.FIRST .. l_grant_mes.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_mes (i) || p_vc_schema;
      END LOOP;
   END prc_grant_mes;

   PROCEDURE prc_revoke_mes (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_mes.FIRST .. l_revoke_mes.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_mes (i) || p_vc_schema;
      END LOOP;
   END prc_revoke_mes;

   PROCEDURE prc_grant_stg (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_grant_stg.FIRST .. l_grant_stg.LAST
      LOOP
         EXECUTE IMMEDIATE l_grant_stg (i) || p_vc_schema;
      END LOOP;
   END prc_grant_stg;

   PROCEDURE prc_revoke_stg (
      p_vc_schema   VARCHAR2
   )
   IS
   BEGIN
      FOR i IN l_revoke_stg.FIRST .. l_revoke_stg.LAST
      LOOP
         EXECUTE IMMEDIATE l_revoke_stg (i) || p_vc_schema;
      END LOOP;
   END prc_revoke_stg;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_enable-impl.sql 2788 2012-05-15 09:00:31Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-impl.sql $';
END authorize;
/

SHOW errors