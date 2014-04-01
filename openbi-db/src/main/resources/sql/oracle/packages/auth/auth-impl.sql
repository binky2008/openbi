CREATE OR REPLACE PACKAGE BODY auth
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   TYPE t_statement IS TABLE OF VARCHAR2 (1000);

   l_grant_tool    t_statement
                      := t_statement (
                            'GRANT EXECUTE ON type TO '
                          , 'GRANT EXECUTE ON dict TO '
                          , 'GRANT EXECUTE ON stmt TO '
                          , 'GRANT EXECUTE ON ddls TO '
                          , 'GRANT EXECUTE ON enbl TO '
                         );
   l_revoke_tool   t_statement
                      := t_statement (
                            'REVOKE EXECUTE ON type FROM '
                          , 'REVOKE EXECUTE ON dict FROM '
                          , 'REVOKE EXECUTE ON stmt FROM '
                          , 'REVOKE EXECUTE ON ddls FROM '
                          , 'REVOKE EXECUTE ON enbl FROM '
                         );
   l_grant_trac    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE ON trac_t TO '
                          , 'GRANT EXECUTE ON trac_param TO '
                          , 'GRANT EXECUTE ON trac TO '
                         );
   l_revoke_trac   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE ON trac_t FROM '
                          , 'REVOKE EXECUTE ON trac_param FROM '
                          , 'REVOKE EXECUTE ON trac FROM '
                         );
   l_grant_mesr    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE,DELETE ON user_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON taxn_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON taxn_user_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON mesr_taxn_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON mesr_query_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON mesr_keyfigure_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON mesr_threshold_t TO '
                          , 'GRANT INSERT,DELETE ON mesr_exec_t TO '
                          , 'GRANT EXECUTE ON mesr TO '
                         );
   l_revoke_mesr   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE,DELETE ON user_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON taxn_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON taxn_user_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON mesr_taxn_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON mesr_query_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON mesr_keyfigure_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON mesr_threshold_t FROM '
                          , 'REVOKE INSERT,DELETE ON mesr_exec_t FROM '
                          , 'REVOKE EXECUTE ON mesr FROM '
                         );
   l_grant_stag    t_statement
                      := t_statement (
                            'GRANT INSERT,UPDATE,DELETE ON stag_stat_type_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_stat_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_size_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_ddl_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_column_tmp TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_source_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_source_db_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_object_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_column_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_column_check_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_queue_t TO '
                          , 'GRANT INSERT,UPDATE,DELETE ON stag_queue_object_t TO '
                          , 'GRANT EXECUTE ON stag_param TO '
                          , 'GRANT EXECUTE ON stag_stat TO '
                          , 'GRANT EXECUTE ON stag_meta TO '
                          , 'GRANT EXECUTE ON stag_ddl TO '
                          , 'GRANT EXECUTE ON stag_build TO '
                          , 'GRANT EXECUTE ON stag_ctl TO '
                         );
   l_revoke_stag   t_statement
                      := t_statement (
                            'REVOKE INSERT,UPDATE,DELETE ON stag_stat_type_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_stat_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_size_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_ddl_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_column_tmp FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_source_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_source_db_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_object_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_column_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_column_check_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_queue_t FROM '
                          , 'REVOKE INSERT,UPDATE,DELETE ON stag_queue_object_t FROM '
                          , 'REVOKE EXECUTE ON stag_param FROM '
                          , 'REVOKE EXECUTE ON stag_stat FROM '
                          , 'REVOKE EXECUTE ON stag_meta FROM '
                          , 'REVOKE EXECUTE ON stag_ddl FROM '
                          , 'REVOKE EXECUTE ON stag_build FROM '
                          , 'REVOKE EXECUTE ON stag_ctl FROM '
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
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END auth;
/

SHOW ERRORS