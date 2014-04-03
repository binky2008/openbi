CREATE OR REPLACE PACKAGE BODY enbl
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   TYPE t_statement IS TABLE OF VARCHAR2 (1000);

   l_synonym_tool   t_statement
                       := t_statement (
                             'type'
                           , 'dict'
                           , 'stmt'
                           , 'ddls'
                          );
   l_synonym_trac   t_statement
                       := t_statement (
                             'trac_param'
                           , 'trac_t'
                           , 'trac'
                          );
   l_synonym_mesr   t_statement
                       := t_statement (
                             'docu_t'
                           , 'user_t'
                           , 'taxn_t'
                           , 'taxn_user_t'
                           , 'mesr_taxn_t'
                           , 'mesr_query_t'
                           , 'mesr_keyfigure_t'
                           , 'mesr_threshold_t'
                           , 'mesr_exec_t'
                           , 'docu'
                           , 'mesr'
                          );
   l_synonym_stag   t_statement
                       := t_statement (
                             'stag_stat_type_t'
                           , 'stag_stat_t'
                           , 'stag_size_t'
                           , 'stag_ddl_t'
                           , 'stag_column_tmp'
                           , 'stag_source_t'
                           , 'stag_source_db_t'
                           , 'stag_object_t'
                           , 'stag_column_t'
                           , 'stag_column_check_t'
                           , 'stag_queue_t'
                           , 'stag_queue_object_t'
                           , 'stag_param'
                           , 'stag_stat'
                           , 'stag_meta'
                           , 'stag_ddl'
                           , 'stag_build'
                           , 'stag_ctl'
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
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END enbl;