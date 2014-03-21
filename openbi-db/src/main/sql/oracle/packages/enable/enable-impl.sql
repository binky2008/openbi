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

   l_synonym_utl      t_statement
      := t_statement ('param'
                    , 'type'
                    , 'dict'
                    , 'ddl'
                     );

   l_synonym_trc      t_statement
      := t_statement ('trc_t'
                    , 'trc'
                     );
   l_synonym_mes       t_statement
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
   l_synonym_stg    t_statement
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