CREATE OR REPLACE PACKAGE BODY p#frm#auth
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: auth-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/auth/auth-impl.sql $
   */
   TYPE t_statement IS TABLE OF VARCHAR2 (1000);

   l_grant_tool    t_statement
                      := t_statement (
                            'GRANT EXECUTE ON p#frm#type TO '
                          , 'GRANT EXECUTE ON p#frm#dict TO '
                          , 'GRANT EXECUTE ON p#frm#stmt TO '
                          , 'GRANT EXECUTE ON p#frm#ddls TO '
                          , 'GRANT EXECUTE ON p#frm#enbl TO '
                         );
   l_revoke_tool   t_statement
                      := t_statement (
                            'REVOKE EXECUTE ON p#frm#type FROM '
                          , 'REVOKE EXECUTE ON p#frm#dict FROM '
                          , 'REVOKE EXECUTE ON p#frm#stmt FROM '
                          , 'REVOKE EXECUTE ON p#frm#ddls FROM '
                          , 'REVOKE EXECUTE ON p#frm#enbl FROM '
                         );
   l_grant_trac    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE ON p#frm#trac_t TO '
                          , 'GRANT EXECUTE ON p#frm#trac_param TO '
                          , 'GRANT EXECUTE ON p#frm#trac TO '
                         );
   l_revoke_trac   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE ON p#frm#trac_t FROM '
                          , 'REVOKE EXECUTE ON p#frm#trac_param FROM '
                          , 'REVOKE EXECUTE ON p#frm#trac FROM '
                         );
   l_grant_mesr    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE,DELETE ON p#frm#user_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#taxn_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#taxn_user_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#mesr_taxn_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#mesr_query_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#mesr_keyfigure_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#mesr_threshold_t TO '
                          , 'GRANT INSERT,DELETE ON p#frm#mesr_exec_t TO '
                          , 'GRANT EXECUTE ON p#frm#mesr TO '
                         );
   l_revoke_mesr   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE,DELETE ON p#frm#user_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#taxn_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#taxn_user_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#mesr_taxn_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#mesr_query_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#mesr_keyfigure_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#mesr_threshold_t FROM '
                          , 'REVOKE INSERT,DELETE ON p#frm#mesr_exec_t FROM '
                          , 'REVOKE EXECUTE ON p#frm#mesr FROM '
                         );
   l_grant_stag    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_stat_type_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_stat_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_size_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_ddl_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_source_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_source_db_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_object_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_column_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_column_check_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_queue_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON p#frm#stag_queue_object_t TO '
                          , 'GRANT EXECUTE ON p#frm#stag_param TO '
                          , 'GRANT EXECUTE ON p#frm#stag_stat TO '
                          , 'GRANT EXECUTE ON p#frm#stag_meta TO '
                          , 'GRANT EXECUTE ON p#frm#stag_ddl TO '
                          , 'GRANT EXECUTE ON p#frm#stag_build TO '
                          , 'GRANT EXECUTE ON p#frm#stag_ctl TO '
                         );
   l_revoke_stag   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_stat_type_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_stat_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_size_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_ddl_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_source_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_source_db_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_object_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_column_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_column_check_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_queue_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON p#frm#stag_queue_object_t FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_param FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_stat FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_meta FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_ddl FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_build FROM '
                          , 'REVOKE EXECUTE ON p#frm#stag_ctl FROM '
                         );

   PROCEDURE prc_grant_tool (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_grant_tool.FIRST .. l_grant_tool.LAST LOOP
         EXECUTE IMMEDIATE
               l_grant_tool (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_revoke_tool (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_revoke_tool.FIRST .. l_revoke_tool.LAST LOOP
         EXECUTE IMMEDIATE
               l_revoke_tool (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_grant_trac (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_grant_trac.FIRST .. l_grant_trac.LAST LOOP
         EXECUTE IMMEDIATE
               l_grant_trac (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_revoke_trac (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_revoke_trac.FIRST .. l_revoke_trac.LAST LOOP
         EXECUTE IMMEDIATE
               l_revoke_trac (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_grant_mesr (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_grant_mesr.FIRST .. l_grant_mesr.LAST LOOP
         EXECUTE IMMEDIATE
               l_grant_mesr (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_revoke_mesr (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_revoke_mesr.FIRST .. l_revoke_mesr.LAST LOOP
         EXECUTE IMMEDIATE
               l_revoke_mesr (i)
            || p_vc_schema;
      END LOOP;
   END;

   PROCEDURE prc_grant_stag (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_grant_stag.FIRST .. l_grant_stag.LAST LOOP
         EXECUTE IMMEDIATE
               l_grant_stag (i)
            || p_vc_schema;
      END LOOP;
   END prc_grant_stag;

   PROCEDURE prc_revoke_stag (p_vc_schema VARCHAR2)
   IS
   BEGIN
      FOR i IN l_revoke_stag.FIRST .. l_revoke_stag.LAST LOOP
         EXECUTE IMMEDIATE
               l_revoke_stag (i)
            || p_vc_schema;
      END LOOP;
   END prc_revoke_stag;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: auth-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/auth/auth-impl.sql $';
END p#frm#auth;