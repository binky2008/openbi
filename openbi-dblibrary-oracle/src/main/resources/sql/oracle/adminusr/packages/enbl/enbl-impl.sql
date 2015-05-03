CREATE OR REPLACE PACKAGE BODY p#frm#enbl
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: enbl-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/enbl/enbl-impl.sql $
   */
   TYPE t_statement IS TABLE OF VARCHAR2 (1000);

   l_synonym_tool   t_statement
                       := t_statement (
                             'p#frm#type'
                           , 'p#frm#dict'
                           , 'p#frm#stmt'
                           , 'p#frm#ddls'
                          );
   l_synonym_trac   t_statement
                       := t_statement (
                             'p#frm#trac_param'
                           , 'p#frm#trac_t'
                           , 'p#frm#trac'
                          );
   l_synonym_mesr   t_statement
                       := t_statement (
                             'p#frm#docu_t'
                           , 'p#frm#user_t'
                           , 'p#frm#taxn_t'
                           , 'p#frm#taxn_user_t'
                           , 'p#frm#mesr_taxn_t'
                           , 'p#frm#mesr_query_t'
                           , 'p#frm#mesr_keyfigure_t'
                           , 'p#frm#mesr_threshold_t'
                           , 'p#frm#mesr_exec_t'
                           , 'p#frm#docu'
                           , 'p#frm#mesr'
                          );
   l_synonym_stag   t_statement
                       := t_statement (
                             'p#frm#stag_stat_type_t'
                           , 'p#frm#stag_stat_t'
                           , 'p#frm#stag_size_t'
                           , 'p#frm#stag_ddl_t'
                           , 'p#frm#stag_source_t'
                           , 'p#frm#stag_source_db_t'
                           , 'p#frm#stag_object_t'
                           , 'p#frm#stag_column_t'
                           , 'p#frm#stag_column_check_t'
                           , 'p#frm#stag_queue_t'
                           , 'p#frm#stag_queue_object_t'
                           , 'p#frm#stag_param'
                           , 'p#frm#stag_stat'
                           , 'p#frm#stag_meta'
                           , 'p#frm#stag_ddl'
                           , 'p#frm#stag_build'
                           , 'p#frm#stag_ctl'
                          );

   /**
   * Common help procedures
   */
   PROCEDURE prc_create_synonym (
      p_vc_tools_owner   IN VARCHAR2
    , p_vc_object_name   IN VARCHAR2
   )
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE
               'DROP SYNONYM '
            || p_vc_object_name;
      EXCEPTION
         WHEN OTHERS THEN
            NULL;
      END;

      EXECUTE IMMEDIATE
            'CREATE SYNONYM '
         || p_vc_object_name
         || ' FOR '
         || p_vc_tools_owner
         || '.'
         || p_vc_object_name;
   END;

   PROCEDURE prc_drop_synonym (p_vc_object_name IN VARCHAR2)
   IS
   BEGIN
      BEGIN
         EXECUTE IMMEDIATE
               'DROP SYNONYM '
            || p_vc_object_name;
      EXCEPTION
         WHEN OTHERS THEN
            NULL;
      END;
   END;

   /**
   * Main procedures
   */
   PROCEDURE prc_enable_tool (p_vc_tools_owner IN VARCHAR2)
   IS
   BEGIN
      FOR i IN l_synonym_tool.FIRST .. l_synonym_tool.LAST LOOP
         prc_create_synonym (
            p_vc_tools_owner
          , l_synonym_tool (i)
         );
      END LOOP;
   END;

   PROCEDURE prc_disable_tool
   IS
   BEGIN
      FOR i IN l_synonym_tool.FIRST .. l_synonym_tool.LAST LOOP
         prc_drop_synonym (l_synonym_tool (i));
      END LOOP;
   END;

   PROCEDURE prc_enable_trac (p_vc_tools_owner IN VARCHAR2)
   IS
   BEGIN
      FOR i IN l_synonym_trac.FIRST .. l_synonym_trac.LAST LOOP
         prc_create_synonym (
            p_vc_tools_owner
          , l_synonym_trac (i)
         );
      END LOOP;
   END;

   PROCEDURE prc_disable_trac
   IS
   BEGIN
      FOR i IN l_synonym_trac.FIRST .. l_synonym_trac.LAST LOOP
         prc_drop_synonym (l_synonym_trac (i));
      END LOOP;
   END;

   PROCEDURE prc_enable_mesr (p_vc_tools_owner IN VARCHAR2)
   IS
   BEGIN
      FOR i IN l_synonym_mesr.FIRST .. l_synonym_mesr.LAST LOOP
         prc_create_synonym (
            p_vc_tools_owner
          , l_synonym_mesr (i)
         );
      END LOOP;
   END;

   PROCEDURE prc_disable_mesr
   IS
   BEGIN
      FOR i IN l_synonym_mesr.FIRST .. l_synonym_mesr.LAST LOOP
         prc_drop_synonym (l_synonym_mesr (i));
      END LOOP;
   END;

   PROCEDURE prc_enable_stag (p_vc_tools_owner IN VARCHAR2)
   IS
   BEGIN
      FOR i IN l_synonym_stag.FIRST .. l_synonym_stag.LAST LOOP
         prc_create_synonym (
            p_vc_tools_owner
          , l_synonym_stag (i)
         );
      END LOOP;
   END;

   PROCEDURE prc_disable_stag
   IS
   BEGIN
      FOR i IN l_synonym_stag.FIRST .. l_synonym_stag.LAST LOOP
         prc_drop_synonym (l_synonym_stag (i));
      END LOOP;
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: enbl-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/enbl/enbl-impl.sql $';
END p#frm#enbl;