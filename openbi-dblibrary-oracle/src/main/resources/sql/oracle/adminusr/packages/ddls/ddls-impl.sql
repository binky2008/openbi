CREATE OR REPLACE PACKAGE BODY p#frm#ddls
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: ddls-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/ddls/ddls-impl.sql $
   */
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

   FUNCTION fct_get_table_migrate_stmt (
      p_vc_table_name_trg    VARCHAR2
    , p_vc_table_name_src    VARCHAR2
   )
      RETURN CLOB
   IS
      l_vc_column_list   VARCHAR2 (32000);
   BEGIN
      l_vc_column_list :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , NULL
          , UPPER (p_vc_table_name_trg)
          , NULL
          , UPPER (p_vc_table_name_src)
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );
      RETURN    'INSERT INTO '
             || p_vc_table_name_trg
             || '('
             || l_vc_column_list
             || ') SELECT '
             || l_vc_column_list
             || ' FROM '
             || p_vc_table_name_src;
   END fct_get_table_migrate_stmt;

   PROCEDURE prc_execute (p_sql_code CLOB)
   IS
      l_vcs_code      DBMS_SQL.varchar2s;
      l_i_cursor_id   INTEGER;
   BEGIN
      l_vcs_code :=
         p#frm#type.fct_clob_to_list (
            p_sql_code
          , CHR (10)
         );
      l_i_cursor_id := DBMS_SQL.open_cursor;
      DBMS_SQL.parse (
         l_i_cursor_id
       , l_vcs_code
       , l_vcs_code.FIRST
       , l_vcs_code.LAST
       , TRUE
       , DBMS_SQL.native
      );
      DBMS_SQL.close_cursor (l_i_cursor_id);
   EXCEPTION
      WHEN OTHERS THEN
         DBMS_SQL.close_cursor (l_i_cursor_id);

         FOR i IN l_vcs_code.FIRST .. l_vcs_code.LAST LOOP
            DBMS_OUTPUT.put_line (l_vcs_code (i));
         END LOOP;

         RAISE;
   END prc_execute;

   PROCEDURE prc_migrate_table (
      p_vc_table_name_trg    VARCHAR2
    , p_vc_table_name_src    VARCHAR2
   )
   IS
   BEGIN
      EXECUTE IMMEDIATE
         fct_get_table_migrate_stmt (
            UPPER (TRIM (p_vc_table_name_trg))
          , UPPER (TRIM (p_vc_table_name_src))
         );

      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END prc_migrate_table;

   PROCEDURE prc_backup_table (
      p_vc_table_name     VARCHAR2
    , p_vc_backup_name    VARCHAR2
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
   BEGIN
      prc_drop_object (
         'TABLE'
       , p_vc_backup_name
      );

      EXECUTE IMMEDIATE
            'CREATE TABLE '
         || p_vc_backup_name
         || ' AS SELECT * FROM '
         || p_vc_table_name;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END prc_backup_table;

   PROCEDURE prc_drop_object (
      p_vc_object_type    VARCHAR2
    , p_vc_object_name    VARCHAR2
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_DROP_OBJECT';
      l_ddl_drop      VARCHAR2 (32000);
   BEGIN
      l_ddl_drop :=
            'DROP '
         || p_vc_object_type
         || ' '
         || p_vc_object_name;

      EXECUTE IMMEDIATE l_ddl_drop;
   EXCEPTION
      WHEN OTHERS THEN
         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_drop_object;

   PROCEDURE prc_create_synonym (
      p_vc_object_name     VARCHAR2
    , p_vc_synonym_name    VARCHAR2
    , p_b_public           BOOLEAN DEFAULT FALSE
    , p_b_drop_flag        BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_object_type   t_object_name;
   BEGIN
      l_vc_object_type :=
            CASE
               WHEN p_b_public THEN
                  'PUBLIC '
            END
         || 'SYNONYM';

      IF p_b_drop_flag THEN
         prc_drop_object (
            l_vc_object_type
          , p_vc_synonym_name
          , FALSE
         );
      END IF;

      EXECUTE IMMEDIATE
            'CREATE '
         || l_vc_object_type
         || ' '
         || p_vc_synonym_name
         || ' FOR '
         || p_vc_object_name;
   EXCEPTION
      WHEN OTHERS THEN
         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_synonym;

   PROCEDURE prc_create_object (
      p_vc_object_type    VARCHAR2
    , p_vc_object_name    VARCHAR2
    , p_vc_object_ddl     CLOB
    , p_b_drop_flag       BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_object_ddl   CLOB;
   BEGIN
      IF p_b_drop_flag
     AND p_vc_object_type NOT IN ('PACKAGE BODY', 'CONSTRAINT') THEN
         prc_drop_object (
            p_vc_object_type
          , p_vc_object_name
          , FALSE
         );
      END IF;

      l_vc_object_ddl := p_vc_object_ddl;

      BEGIN
         prc_execute (l_vc_object_ddl);
      EXCEPTION
         WHEN OTHERS THEN
            IF p_b_raise_flag THEN
               RAISE;
            END IF;
      END;
   END;

   PROCEDURE prc_create_entity (
      p_vc_entity_prefix    VARCHAR2
    , p_vc_entity_name      VARCHAR2
    , p_vc_entity_fields    VARCHAR2
    , p_vc_create_mode      VARCHAR2 DEFAULT 'DEFAULT'
    , p_b_public_flag       BOOLEAN DEFAULT FALSE
    , p_b_migrate_flag      BOOLEAN DEFAULT FALSE
    , p_b_cdc_flag          BOOLEAN DEFAULT FALSE
   )
   IS
      l_name_entity_tab   VARCHAR2 (100);
      l_name_entity_cdc   VARCHAR2 (100);
      l_name_entity_bkp   VARCHAR2 (100);
      l_name_entity_cbk   VARCHAR2 (100);
      l_name_entity_seq   VARCHAR2 (100);
      l_name_entity_id    VARCHAR2 (100);
      l_name_entity_pk    VARCHAR2 (100);
      l_columns_all       CLOB;
      l_columns_old       CLOB;
      l_columns_new       CLOB;
      l_sql_create        CLOB;
      l_n_cnt_tab         NUMBER;
      l_n_cnt_hst         NUMBER;
   BEGIN
      -- Set name of physical objects
      l_name_entity_tab := c_name_entity_tab;
      prc_set_text_param (
         l_name_entity_tab
       , 'entityPrefix'
       , p_vc_entity_prefix
      );
      prc_set_text_param (
         l_name_entity_tab
       , 'entityName'
       , p_vc_entity_name
      );
      --
      l_name_entity_cdc := c_name_entity_cdc;
      prc_set_text_param (
         l_name_entity_cdc
       , 'entityPrefix'
       , p_vc_entity_prefix
      );
      prc_set_text_param (
         l_name_entity_cdc
       , 'entityName'
       , p_vc_entity_name
      );
      --
      l_name_entity_seq := c_name_entity_seq;
      prc_set_text_param (
         l_name_entity_seq
       , 'entityPrefix'
       , p_vc_entity_prefix
      );
      prc_set_text_param (
         l_name_entity_seq
       , 'entityName'
       , p_vc_entity_name
      );
      --
      l_name_entity_id := c_name_entity_id;
      prc_set_text_param (
         l_name_entity_id
       , 'entityPrefix'
       , p_vc_entity_prefix
      );
      prc_set_text_param (
         l_name_entity_id
       , 'entityName'
       , p_vc_entity_name
      );
      --
      l_name_entity_pk := c_name_entity_pk;
      prc_set_text_param (
         l_name_entity_pk
       , 'entityPrefix'
       , p_vc_entity_prefix
      );
      prc_set_text_param (
         l_name_entity_pk
       , 'entityName'
       , p_vc_entity_name
      );

      IF p_b_migrate_flag THEN
         SELECT COUNT (0)
           INTO l_n_cnt_tab
           FROM user_tables
          WHERE table_name = TRIM (UPPER (l_name_entity_tab));

         IF l_n_cnt_tab > 0 THEN
            l_name_entity_bkp := c_name_entity_bkp;
            prc_set_text_param (
               l_name_entity_bkp
             , 'entityPrefix'
             , p_vc_entity_prefix
            );
            prc_set_text_param (
               l_name_entity_bkp
             , 'entityName'
             , p_vc_entity_name
            );
            prc_backup_table (
               l_name_entity_tab
             , l_name_entity_bkp
            );
         END IF;
      END IF;

      IF p_b_migrate_flag
     AND p_b_cdc_flag THEN
         SELECT COUNT (0)
           INTO l_n_cnt_hst
           FROM user_tables
          WHERE table_name = TRIM (UPPER (l_name_entity_cdc));

         IF l_n_cnt_hst > 0 THEN
            l_name_entity_cbk := c_name_entity_cbk;
            prc_set_text_param (
               l_name_entity_cbk
             , 'entityPrefix'
             , p_vc_entity_prefix
            );
            prc_set_text_param (
               l_name_entity_cbk
             , 'entityName'
             , p_vc_entity_name
            );
            prc_backup_table (
               l_name_entity_cdc
             , l_name_entity_cbk
            );
         END IF;
      END IF;

      -- Drop physical objects if required
      IF p_vc_create_mode = 'DROP' THEN
         -- Drop table
         prc_drop_object (
            'TABLE'
          , l_name_entity_tab
         );

         IF NOT p_b_migrate_flag THEN
            -- Drop sequence
            prc_drop_object (
               'SEQUENCE'
             , l_name_entity_seq
            );
         END IF;
      END IF;

      IF p_vc_create_mode = 'DROP'
     AND p_b_cdc_flag THEN
         -- Drop table
         prc_drop_object (
            'TABLE'
          , l_name_entity_cdc
         );
      END IF;

      -- Create table
      l_sql_create := c_template_entity_tab;
      prc_set_text_param (
         l_sql_create
       , 'entityTable'
       , l_name_entity_tab
      );
      prc_set_text_param (
         l_sql_create
       , 'entityId'
       , l_name_entity_id
      );
      prc_set_text_param (
         l_sql_create
       , 'entityPK'
       , l_name_entity_pk
      );
      prc_set_text_param (
         l_sql_create
       , 'columnDefinitionList'
       , p_vc_entity_fields
      );
      prc_execute (l_sql_create);

      IF p_b_cdc_flag THEN
         -- Create CDC table
         l_sql_create := c_template_entity_cdc;
         prc_set_text_param (
            l_sql_create
          , 'entityCDC'
          , l_name_entity_cdc
         );
         prc_set_text_param (
            l_sql_create
          , 'entityId'
          , l_name_entity_id
         );
         prc_set_text_param (
            l_sql_create
          , 'columnDefinitionList'
          , p_vc_entity_fields
         );
         prc_execute (l_sql_create);
      END IF;

      -- Create sequence
      l_sql_create := c_template_entity_seq;
      prc_set_text_param (
         l_sql_create
       , 'entitySequence'
       , l_name_entity_seq
      );

      BEGIN
         prc_execute (l_sql_create);
      EXCEPTION
         WHEN OTHERS THEN
            NULL;
      END;

      -- Create triggers
      l_sql_create := c_template_entity_trg_ins;
      prc_set_text_param (
         l_sql_create
       , 'entityTable'
       , l_name_entity_tab
      );
      prc_set_text_param (
         l_sql_create
       , 'entityId'
       , l_name_entity_id
      );
      prc_set_text_param (
         l_sql_create
       , 'entitySequence'
       , l_name_entity_seq
      );
      prc_execute (l_sql_create);
      l_sql_create := c_template_entity_trg_upd;
      prc_set_text_param (
         l_sql_create
       , 'entityTable'
       , l_name_entity_tab
      );
      prc_execute (l_sql_create);

      IF p_b_public_flag THEN
         EXECUTE IMMEDIATE
               'GRANT SELECT ON '
            || l_name_entity_tab
            || ' TO PUBLIC';
      END IF;

      IF p_b_migrate_flag
     AND l_n_cnt_tab > 0 THEN
         -- Migrate content
         prc_migrate_table (
            l_name_entity_tab
          , l_name_entity_bkp
         );
      END IF;

      IF p_b_migrate_flag
     AND p_b_cdc_flag
     AND l_n_cnt_hst > 0 THEN
         -- Migrate history content
         prc_migrate_table (
            l_name_entity_cdc
          , l_name_entity_cbk
         );
      END IF;

      IF p_b_cdc_flag THEN
         -- Create CDC trigger
         l_columns_all :=
            p#frm#dict.fct_get_column_list (
               NULL
             , NULL
             , UPPER (l_name_entity_tab)
             , 'ALL'
             , 'LIST_SIMPLE'
            );
         l_columns_old :=
            p#frm#dict.fct_get_column_list (
               NULL
             , NULL
             , UPPER (l_name_entity_tab)
             , 'ALL'
             , 'LIST_ALIAS'
             , ':OLD'
            );
         l_columns_new :=
            p#frm#dict.fct_get_column_list (
               NULL
             , NULL
             , UPPER (l_name_entity_tab)
             , 'ALL'
             , 'LIST_ALIAS'
             , ':NEW'
            );
         l_sql_create := c_template_entity_trg_cdc;
         prc_set_text_param (
            l_sql_create
          , 'entityTable'
          , l_name_entity_tab
         );
         prc_set_text_param (
            l_sql_create
          , 'entityCDC'
          , l_name_entity_cdc
         );
         prc_set_text_param (
            l_sql_create
          , 'columnList'
          , l_columns_all
         );
         prc_set_text_param (
            l_sql_create
          , 'columnListOld'
          , l_columns_old
         );
         prc_set_text_param (
            l_sql_create
          , 'columnListNew'
          , l_columns_new
         );
         prc_execute (l_sql_create);
      END IF;
   END prc_create_entity;
BEGIN
   -- Versioning constants
   c_body_version := '$Id: ddls-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/ddls/ddls-impl.sql $';
END p#frm#ddls;