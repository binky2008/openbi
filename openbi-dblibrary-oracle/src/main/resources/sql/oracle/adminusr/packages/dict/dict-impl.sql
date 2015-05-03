CREATE OR REPLACE PACKAGE BODY p#frm#dict
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: dict-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/dict/dict-impl.sql $
   */
   FUNCTION fct_column_in_list (
      p_vc_column_name    VARCHAR2
    , p_vc_column_list    VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_l_column_list   DBMS_SQL.varchar2s;
      l_b_is_in_list    BOOLEAN := FALSE;
   BEGIN
      l_l_column_list :=
         p#frm#type.fct_string_to_list (
            p_vc_column_list
          , ','
         );

      FOR i IN l_l_column_list.FIRST .. l_l_column_list.LAST LOOP
         IF p_vc_column_name = l_l_column_list (i) THEN
            l_b_is_in_list := TRUE;
         END IF;
      END LOOP;

      RETURN l_b_is_in_list;
   END;

   PROCEDURE prc_set_text_param (
      p_vc_code_string   IN OUT CLOB
    , p_vc_param_name    IN     VARCHAR2
    , p_vc_param_value   IN     CLOB
   )
   IS
      l_vc_prc_name        t_object_name := 'PRC_SET_TEXT_PARAM';
      l_vc_buffer_in       CLOB;
      l_vc_buffer_out      CLOB;
      l_vc_token           CLOB;
      l_i_position_begin   INTEGER;
      l_i_position_end     INTEGER;
   BEGIN
      l_vc_buffer_in := p_vc_code_string;
      l_i_position_begin :=
           INSTR (
              l_vc_buffer_in
            ,    '#'
              || p_vc_param_name
              || '#'
           )
         - 1;
      l_i_position_end :=
           INSTR (
              l_vc_buffer_in
            ,    '#'
              || p_vc_param_name
              || '#'
           )
         + LENGTH (p_vc_param_name)
         + 2;

      -- Loop on occurencies of the parameter into the root code
      WHILE l_i_position_begin >= 0 LOOP
         l_vc_token :=
            SUBSTR (
               l_vc_buffer_in
             , 1
             , l_i_position_begin
            );
         l_vc_buffer_out :=
               l_vc_buffer_out
            || l_vc_token;
         l_vc_buffer_out :=
               l_vc_buffer_out
            || p_vc_param_value;
         l_vc_buffer_in :=
            SUBSTR (
               l_vc_buffer_in
             , l_i_position_end
            );
         l_i_position_begin :=
              INSTR (
                 l_vc_buffer_in
               ,    '#'
                 || p_vc_param_name
                 || '#'
              )
            - 1;
         l_i_position_end :=
              INSTR (
                 l_vc_buffer_in
               ,    '#'
                 || p_vc_param_name
                 || '#'
              )
            + LENGTH (p_vc_param_name)
            + 2;
      END LOOP;

      -- Append the rest token
      l_vc_buffer_out :=
            l_vc_buffer_out
         || l_vc_buffer_in;
      p_vc_code_string := l_vc_buffer_out;
   EXCEPTION
      WHEN OTHERS THEN
         RAISE;
   END prc_set_text_param;

   PROCEDURE prc_set_src_param (p_vc_code_string IN OUT CLOB)
   IS
      l_vc_prc_name   t_string := 'PRC_SET_SRC_PARAM';
   BEGIN
      prc_set_text_param (
         p_vc_code_string
       , 'owner'
       , CASE
            WHEN g_vc_src_obj_owner IS NOT NULL THEN
                  g_vc_src_obj_owner
               || '.'
         END
      );
      prc_set_text_param (
         p_vc_code_string
       , 'dblink'
       , CASE
            WHEN g_vc_src_obj_dblink IS NOT NULL THEN
                  '@'
               || g_vc_src_obj_dblink
         END
      );
   END prc_set_src_param;

   PROCEDURE prc_import_metadata (
      p_vc_dblink               VARCHAR2
    , p_vc_owner                VARCHAR2
    , p_vc_object_name          VARCHAR2
    , p_vc_target_object        VARCHAR2
    , p_vc_target_columns       VARCHAR2 DEFAULT NULL
    , p_b_check_dependencies    BOOLEAN DEFAULT TRUE
   )
   IS
      l_sql_col_def           t_string := c_sql_col_def;
      l_sql_import_metadata   t_string := c_sql_import_metadata;
   BEGIN
      g_vc_src_obj_dblink := p_vc_dblink;
      prc_set_text_param (
         l_sql_col_def
       , 'sql_obj_pk'
       , CASE
            WHEN p_b_check_dependencies THEN
               c_sql_obj_col_pk
            ELSE
               c_sql_obj_col_pk_nodep
         END
      );
      prc_set_src_param (l_sql_col_def);
      prc_set_text_param (
         l_sql_import_metadata
       , 'targetObject'
       , p_vc_target_object
      );
      prc_set_text_param (
         l_sql_import_metadata
       , 'targetColumns'
       , CASE
            WHEN p_vc_target_columns IS NOT NULL THEN
                  '('
               || p_vc_target_columns
               || ')'
         END
      );
      prc_set_text_param (
         l_sql_import_metadata
       , 'sourceSelect'
       , l_sql_col_def
      );

      EXECUTE IMMEDIATE l_sql_import_metadata
         USING p_vc_owner
             , p_vc_object_name;

      COMMIT;
   END prc_import_metadata;

   FUNCTION fct_get_table_comment (
      p_vc_dblink         VARCHAR2
    , p_vc_owner          VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN VARCHAR2
   IS
      l_sql_tab_comm   t_string := c_sql_tab_comm;
      l_vc_tab_comm    t_string;
   BEGIN
      g_vc_src_obj_dblink := p_vc_dblink;
      prc_set_src_param (l_sql_tab_comm);

      EXECUTE IMMEDIATE l_sql_tab_comm
         INTO l_vc_tab_comm
         USING p_vc_owner
             , p_vc_object_name;

      ROLLBACK;
      RETURN l_vc_tab_comm;
   END fct_get_table_comment;

   FUNCTION fct_get_column_list (
      p_vc_dblink          VARCHAR2
    , p_vc_owner           VARCHAR2
    , p_vc_object_name     VARCHAR2
    , p_vc_column_type     VARCHAR2
    , p_vc_list_type       VARCHAR2
    , p_vc_alias1          VARCHAR2 DEFAULT NULL
    , p_vc_alias2          VARCHAR2 DEFAULT NULL
    , p_vc_exclude_list    VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      TYPE t_cur_ref IS REF CURSOR;

      l_cur_ref       t_cur_ref;
      l_sql_col_all   t_string := c_sql_col_all;
      l_sql_col_npk   t_string := c_sql_col_npk;
      l_sql_col_pk    t_string := c_sql_col_pk;
      l_vc_buffer     t_string;
      l_vc_list       t_string;
      l_vc_owner      t_object_name;
   BEGIN
      g_vc_src_obj_dblink := p_vc_dblink;
      l_vc_owner := NVL (p_vc_owner, USER);

      IF p_vc_column_type = 'ALL' THEN
         prc_set_src_param (l_sql_col_all);

         OPEN l_cur_ref FOR l_sql_col_all
            USING l_vc_owner
                , p_vc_object_name;
      ELSIF p_vc_column_type = 'PK' THEN
         prc_set_text_param (
            l_sql_col_pk
          , 'sql_obj_pk'
          , c_sql_obj_col_pk
         );
         prc_set_src_param (l_sql_col_pk);

         OPEN l_cur_ref FOR l_sql_col_pk
            USING l_vc_owner
                , p_vc_object_name;
      ELSIF p_vc_column_type = 'NPK' THEN
         prc_set_text_param (
            l_sql_col_npk
          , 'sql_obj_pk'
          , c_sql_obj_col_pk
         );
         prc_set_src_param (l_sql_col_npk);

         OPEN l_cur_ref FOR l_sql_col_npk
            USING l_vc_owner
                , p_vc_object_name
                , l_vc_owner
                , p_vc_object_name;
      END IF;

      LOOP
         FETCH l_cur_ref INTO l_vc_buffer;

         EXIT WHEN l_cur_ref%NOTFOUND;

         IF NOT fct_column_in_list (
                   l_vc_buffer
                 , p_vc_exclude_list
                ) THEN
            l_vc_list :=
                  l_vc_list
               || CHR (10)
               || CASE p_vc_list_type
                     WHEN 'LIST_SIMPLE' THEN
                           l_vc_buffer
                        || ', '
                     WHEN 'LIST_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                     WHEN 'SET_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ' = '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ', '
                     WHEN 'LIST_NVL2' THEN
                           'NVL2 ('
                        || p_vc_alias1
                        || '.rowid, '
                        || p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ') AS '
                        || l_vc_buffer
                        || ', '
                     WHEN 'AND_NOTNULL' THEN
                           l_vc_buffer
                        || ' IS NOT NULL AND '
                     WHEN 'AND_NULL' THEN
                           l_vc_buffer
                        || ' IS NOT NULL AND '
                     WHEN 'AND_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ' = '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ' AND '
                     WHEN 'OR_DECODE' THEN
                           'DECODE ('
                        || p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ', 0, 1) = 1 OR '
                  END;
         END IF;
      END LOOP;

      CLOSE l_cur_ref;

      IF p_vc_list_type IN ('LIST_SIMPLE', 'LIST_ALIAS', 'LIST_NVL2', 'SET_ALIAS') THEN
         l_vc_list :=
            RTRIM (
               l_vc_list
             , ', '
            );
      ELSIF p_vc_list_type IN ('AND_NOTNULL', 'AND_NULL', 'AND_ALIAS') THEN
         l_vc_list :=
            SUBSTR (
               l_vc_list
             , 1
             ,   LENGTH (l_vc_list)
               - 5
            );
      ELSIF p_vc_list_type = 'OR_DECODE' THEN
         l_vc_list :=
            SUBSTR (
               l_vc_list
             , 1
             ,   LENGTH (l_vc_list)
               - 4
            );
      END IF;

      ROLLBACK;
      RETURN l_vc_list;
   END fct_get_column_list;

   FUNCTION fct_get_column_subset (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
    , p_vc_column_type     VARCHAR2
    , p_vc_list_type       VARCHAR2
    , p_vc_alias1          VARCHAR2 DEFAULT NULL
    , p_vc_alias2          VARCHAR2 DEFAULT NULL
    , p_vc_exclude_list    VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      TYPE t_cur_ref IS REF CURSOR;

      l_cur_ref              t_cur_ref;
      l_sql_col_common_all   t_string := c_sql_col_common_all;
      l_sql_col_common_npk   t_string := c_sql_col_common_npk;
      l_vc_buffer            t_string;
      l_vc_list              t_string;
      --
      l_vc_owner1            t_object_name;
      l_vc_owner2            t_object_name;
   BEGIN
      g_vc_src_obj_dblink := p_vc_dblink1;
      l_vc_owner1 := NVL (p_vc_owner1, USER);
      l_vc_owner2 := NVL (p_vc_owner2, USER);

      IF p_vc_column_type = 'COMMON_ALL' THEN
         prc_set_src_param (l_sql_col_common_all);

         OPEN l_cur_ref FOR l_sql_col_common_all
            USING l_vc_owner1
                , p_vc_object1_name
                , l_vc_owner2
                , p_vc_object2_name;
      ELSIF p_vc_column_type = 'COMMON_NPK' THEN
         prc_set_text_param (
            l_sql_col_common_npk
          , 'sql_obj_pk'
          , c_sql_obj_col_pk
         );
         prc_set_src_param (l_sql_col_common_npk);

         OPEN l_cur_ref FOR l_sql_col_common_npk
            USING l_vc_owner1
                , p_vc_object1_name
                , l_vc_owner1
                , p_vc_object1_name
                , l_vc_owner2
                , p_vc_object2_name
                , l_vc_owner2
                , p_vc_object2_name
                , l_vc_owner2
                , p_vc_object2_name;
      END IF;

      LOOP
         FETCH l_cur_ref INTO l_vc_buffer;

         EXIT WHEN l_cur_ref%NOTFOUND;

         IF NOT fct_column_in_list (
                   l_vc_buffer
                 , p_vc_exclude_list
                ) THEN
            l_vc_list :=
                  l_vc_list
               || CHR (10)
               || CASE p_vc_list_type
                     WHEN 'LIST_SIMPLE' THEN
                           l_vc_buffer
                        || ', '
                     WHEN 'LIST_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                     WHEN 'SET_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ' = '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ', '
                     WHEN 'AND_ALIAS' THEN
                           p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ' = '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ' AND '
                     WHEN 'LIST_NVL2' THEN
                           'NVL2 ('
                        || p_vc_alias1
                        || '.rowid, '
                        || p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ') AS '
                        || l_vc_buffer
                        || ', '
                     WHEN 'OR_DECODE' THEN
                           'DECODE ('
                        || p_vc_alias1
                        || '.'
                        || l_vc_buffer
                        || ', '
                        || p_vc_alias2
                        || '.'
                        || l_vc_buffer
                        || ', 0, 1) = 1 OR '
                  END;
         END IF;
      END LOOP;

      CLOSE l_cur_ref;

      IF p_vc_list_type IN ('LIST_SIMPLE', 'LIST_ALIAS', 'LIST_NVL2', 'SET_ALIAS') THEN
         l_vc_list :=
            RTRIM (
               l_vc_list
             , ', '
            );
      ELSIF p_vc_list_type = 'AND_ALIAS' THEN
         l_vc_list :=
            SUBSTR (
               l_vc_list
             , 1
             ,   LENGTH (l_vc_list)
               - 5
            );
      ELSIF p_vc_list_type = 'OR_DECODE' THEN
         l_vc_list :=
            SUBSTR (
               l_vc_list
             , 1
             ,   LENGTH (l_vc_list)
               - 4
            );
      END IF;

      ROLLBACK;
      RETURN l_vc_list;
   END fct_get_column_subset;

   FUNCTION fct_check_part (
      p_vc_dblink         VARCHAR2
    , p_vc_owner          VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_n_cnt_part     NUMBER;
      l_sql_tab_part   t_string := c_sql_tab_part;
   BEGIN
      g_vc_src_obj_dblink := p_vc_dblink;
      prc_set_src_param (l_sql_tab_part);

      EXECUTE IMMEDIATE l_sql_tab_part
         INTO l_n_cnt_part
         USING p_vc_owner
             , p_vc_object_name;

      ROLLBACK;

      IF l_n_cnt_part = 0 THEN
         RETURN FALSE;
      ELSE
         RETURN TRUE;
      END IF;
   END fct_check_part;

   FUNCTION fct_check_col (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_vc_col_all_1   t_string;
      l_vc_col_all_2   t_string;
   BEGIN
      NULL;
   END fct_check_col;

   FUNCTION fct_check_pk (
      p_vc_dblink1         VARCHAR2
    , p_vc_owner1          VARCHAR2
    , p_vc_object1_name    VARCHAR2
    , p_vc_owner2          VARCHAR2
    , p_vc_object2_name    VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_vc_col_pk_1   t_string;
      l_vc_col_pk_2   t_string;
   BEGIN
      l_vc_col_pk_1 :=
         fct_get_column_list (
            p_vc_dblink1
          , p_vc_owner1
          , p_vc_object1_name
          , 'PK'
          , 'LIST_SIMPLE'
         );
      l_vc_col_pk_2 :=
         fct_get_column_list (
            NULL
          , p_vc_owner2
          , p_vc_object2_name
          , 'PK'
          , 'LIST_SIMPLE'
         );
      ROLLBACK;

      IF l_vc_col_pk_1 = l_vc_col_pk_2
      OR (l_vc_col_pk_1 IS NULL
      AND l_vc_col_pk_2 IS NULL) THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END fct_check_pk;
BEGIN
   -- Versioning constants
   c_body_version := '$Id: dict-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/dict/dict-impl.sql $';
END p#frm#dict;