CREATE OR REPLACE PACKAGE BODY pkg_etl_core
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2011-10-20 13:10:16 +0200 (Do, 20 Okt 2011) $
    * $Revision: 1631 $
    * $Id: pkg_etl_core-impl.sql 1631 2011-10-20 11:10:16Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_core/pkg_etl_core-impl.sql $
    */
   -- Constant for development-mode
   -- is set during package initialization
   c_devel_mode                        BOOLEAN;
   -- Constant for package name
   -- is set during package initialization
   c_package_name                      VARCHAR2 (35);
   -- Set hash area size (and sort_area_size)
   c_hash_area                CONSTANT NUMBER        := 9000;
   c_param_delimiter                   VARCHAR2 (1)  := '|';
   c_core_db_link             CONSTANT VARCHAR2 (35) := '';
   c_core_user                CONSTANT VARCHAR2 (35) := 'EDWH_CORE';
   c_cls_user                 CONSTANT VARCHAR2 (35) := 'EDWH_CL';
   g_ret_ok                   CONSTANT NUMBER        := 0;
   g_ret_nok                  CONSTANT NUMBER        := -1;
   g_ret_no_table_lock        CONSTANT NUMBER        := -2;

   -- Types
   TYPE column_t IS RECORD (
      column_id        NUMBER
    , table_name       VARCHAR2 (30)
    , column_name      VARCHAR2 (30)
    , data_type        VARCHAR2 (106)
    , data_length      NUMBER
    , data_precision   NUMBER
    , data_scale       NUMBER
    , nullable         VARCHAR2 (1)
   );

   TYPE column_tab_t IS TABLE OF column_t;

   TYPE column_tab_idx_t IS TABLE OF column_t
      INDEX BY VARCHAR2 (30);

   TYPE numlist IS TABLE OF NUMBER;

   c_sql_get_max_exec_id      CONSTANT CLOB
      := 'SELECT /*+ PARALLEL (a 5) */ NVL(MAX(#columnExecIdUpd#)-1,-999), NVL(MAX(#columnValidFrom#)
     , TO_DATE(''11111111'',''yyyymmdd'')) 
  FROM #tableName# a
 WHERE #columnActiveVersion# = 0';
   c_sql_master_prop_needed   CONSTANT CLOB
      := 'SELECT --+ ordered use_hash (src tgt) full(src) parallel(src 3) 
       COUNT(*)
  FROM #sourceTable# src
     , #targetTable# tgt
 -- Antijoin source vs. target
 WHERE #nkNotNullClause#
   AND #nkJoinClause#';
   c_sql_master_ins           CONSTANT CLOB
      := 'INSERT INTO #targettable#
            (#columnid#
           , di_gui
           , di_gui_ins
           , di_system
           , #columnnktarget#
            )
   SELECT #sequenceidentifier#
        , di_gui
        , di_gui_ins
        , di_system
        , #columnnksource#
     FROM (SELECT DISTINCT
                           --+ ordered use_hash (src tgt) full(src) parallel(src 3) index(tgt)
                           #colimnexecid# AS di_gui
                         , #colimnexecid# AS di_gui_ins
                         , ''from #sourceTable#'' AS di_system
                         , #columnnksource#
                      FROM #sourcetable# src
                         , #targettable# tgt
                     -- Antijoin source vs. target
           WHERE           #nknotnullclause#
                       AND #nkjoinclause#)';

   -- Private functions
   FUNCTION get_max_di_gui (
      p_table_name       IN       VARCHAR2
    , p_max_valid_from   OUT      DATE
   )
      RETURN NUMBER
   IS
      l_max_di_gui       NUMBER;
      l_max_valid_from   DATE;
      l_sql              CLOB   := c_sql_get_max_exec_id;
   BEGIN
      ddl.prc_set_tech_column (l_sql);
      ddl.prc_set_text_param (l_sql
                                    , 'tableName'
                                    , p_table_name
                                     );
      pkg_utl_doc.prc_save_document ('SQL_GET_MAX_GUI'
                                   , 'SQL'
                                   , l_sql
                                   , NULL
                                   , 'Get max GUI'
                                    );

      EXECUTE IMMEDIATE l_sql
                   INTO l_max_di_gui
                      , p_max_valid_from;

      log.LOG ('max(di_gui)     in table ' || c_core_user || '.' || p_table_name || ' is : ' || l_max_di_gui);
      log.LOG ('max(valid_from) in table ' || c_core_user || '.' || p_table_name || ' is : ' || TO_CHAR (p_max_valid_from, 'DD.MM.YYYY'));
      RETURN l_max_di_gui;
   END get_max_di_gui;

   FUNCTION validate_nk_columns (
      p_src_table_name      IN       VARCHAR2
    , p_tgt_table_name      IN       VARCHAR2
    , p_src_nk_column_tab   IN       column_tab_t
    , p_tgt_nk_column_tab   IN       column_tab_t
    , p_log_message         OUT      VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_cnt                 NUMBER;
      l_col_rec             column_t;
      l_src_nk_column_tab   column_tab_t;
      l_tgt_nk_column_tab   column_tab_t;
   BEGIN
      p_log_message          := '';
      l_src_nk_column_tab    := p_src_nk_column_tab;
      l_tgt_nk_column_tab    := p_tgt_nk_column_tab;

      -- (1) Check that NK Column lists are not empty (src and tgt)
      IF    l_src_nk_column_tab.COUNT = 0
         OR l_tgt_nk_column_tab.COUNT = 0
      THEN
         p_log_message    := 'at least on NK column list is empty (src=' || l_src_nk_column_tab.COUNT || '|tgt=' || l_tgt_nk_column_tab.COUNT || ')';
         RETURN FALSE;
      END IF;

      -- (2) Check that number of NL Columns is the same for src and tgt
      IF l_src_nk_column_tab.COUNT != l_tgt_nk_column_tab.COUNT
      THEN
         p_log_message    := 'Number of NK columns does not fit: (src=' || l_src_nk_column_tab.COUNT || '|tgt=' || l_tgt_nk_column_tab.COUNT || ')';
         RETURN FALSE;
      END IF;

      -- (3) Check that source columns are part of the src table
      FOR i IN 1 .. l_src_nk_column_tab.COUNT
      LOOP
         SELECT COUNT (*)
           INTO l_cnt
           FROM all_tab_columns
          WHERE owner = c_cls_user
            AND table_name = p_src_table_name
            AND column_name = l_src_nk_column_tab (i).column_name;

         IF l_cnt != 1
         THEN
            p_log_message    := 'CLS Table ' || p_src_table_name || ' does not contain NK column ' || l_src_nk_column_tab (i).column_name;
            RETURN FALSE;
         END IF;
      END LOOP;

      -- (3a) Enrich source columns
      FOR i IN 1 .. l_src_nk_column_tab.COUNT
      LOOP
         SELECT column_id
              , table_name
              , column_name
              , data_type
              , CASE
                   WHEN char_length = 0
                      THEN data_length
                   ELSE char_length
                END data_length
              , data_precision
              , data_scale
              , nullable
           INTO l_col_rec
           FROM all_tab_columns
          WHERE owner = c_cls_user
            AND table_name = p_src_table_name
            AND column_name = l_src_nk_column_tab (i).column_name;

         l_src_nk_column_tab (i).data_type      := l_col_rec.data_type;
         l_src_nk_column_tab (i).data_length    := l_col_rec.data_length;
      END LOOP;

      -- (4) Check that target columns are part of the tgt table
      FOR i IN 1 .. l_tgt_nk_column_tab.COUNT
      LOOP
         SELECT COUNT (*)
           INTO l_cnt
           -- toDo  > remove DB-Link
         FROM   all_tab_columns
          WHERE owner = c_core_user
            AND table_name = p_tgt_table_name
            AND column_name = l_tgt_nk_column_tab (i).column_name;

         IF l_cnt != 1
         THEN
            p_log_message    := 'Master Table ' || p_tgt_table_name || ' does not contain NK column ' || p_tgt_nk_column_tab (i).column_name;
            RETURN FALSE;
         END IF;
      END LOOP;

      -- (4a) Enrich target columns
      FOR i IN 1 .. l_tgt_nk_column_tab.COUNT
      LOOP
         SELECT column_id
              , table_name
              , column_name
              , data_type
              , CASE
                   WHEN char_length = 0
                      THEN data_length
                   ELSE char_length
                END data_length
              , data_precision
              , data_scale
              , nullable
           INTO l_col_rec
           -- toDo  > remove DB-Link
         FROM   all_tab_columns
          WHERE owner = c_core_user
            AND table_name = p_tgt_table_name
            AND column_name = l_tgt_nk_column_tab (i).column_name;

         l_tgt_nk_column_tab (i).data_type      := l_col_rec.data_type;
         l_tgt_nk_column_tab (i).data_length    := l_col_rec.data_length;
      END LOOP;

      -- (5) Check that Data Types are the same
      FOR i IN 1 .. l_src_nk_column_tab.COUNT
      LOOP
         IF l_src_nk_column_tab (i).data_type != l_tgt_nk_column_tab (i).data_type
         THEN
            p_log_message    :=
               (   'Data Type does not match: '
                || l_src_nk_column_tab (i).column_name
                || '('
                || l_src_nk_column_tab (i).data_type
                || ' '
                || l_src_nk_column_tab (i).data_length
                || ')|'
                || l_tgt_nk_column_tab (i).column_name
                || '('
                || l_tgt_nk_column_tab (i).data_type
                || ' '
                || l_tgt_nk_column_tab (i).data_length
                || ')'
               );
            RETURN FALSE;
         END IF;
      END LOOP;

      -- (6) Check that Data Length is the same
      FOR i IN 1 .. l_src_nk_column_tab.COUNT
      LOOP
         IF l_src_nk_column_tab (i).data_length != l_tgt_nk_column_tab (i).data_length
         THEN
            p_log_message    :=
               (   'Data Length does not match: '
                || l_src_nk_column_tab (i).column_name
                || '('
                || l_src_nk_column_tab (i).data_type
                || ' '
                || l_src_nk_column_tab (i).data_length
                || ')|'
                || l_tgt_nk_column_tab (i).column_name
                || '('
                || l_tgt_nk_column_tab (i).data_type
                || ' '
                || l_tgt_nk_column_tab (i).data_length
                || ')'
               );
            RETURN FALSE;
         END IF;
      END LOOP;

      RETURN TRUE;
   END validate_nk_columns;

   FUNCTION stage_table_exists (
      p_table_name   IN   VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_cnt   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_cnt
        -- toDo  > remove DB-Link
      FROM   all_tables
       WHERE owner = 'STAGE'
         AND table_name = p_table_name;

      IF l_cnt = 1
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END stage_table_exists;

   FUNCTION cls_table_exists (
      p_table_name   IN   VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_cnt   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_cnt
        -- toDo  > remove DB-Link
      FROM   all_tables
       WHERE owner = c_cls_user
         AND table_name = p_table_name;

      IF l_cnt = 1
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END cls_table_exists;

   FUNCTION core_table_exists (
      p_table_name   IN   VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_cnt   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_cnt
        -- toDo  > remove DB-Link
      FROM   all_tables
       WHERE owner = c_core_user
         AND table_name = p_table_name;

      IF l_cnt = 1
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END core_table_exists;

   FUNCTION to_string (
      p_column_tab   IN   column_tab_t
    , p_prefix            VARCHAR2 DEFAULT NULL
    , p_lpad              VARCHAR2 DEFAULT ''
   )
      RETURN VARCHAR2
   IS
      l_column_string   VARCHAR2 (10000);
   BEGIN
      IF p_prefix IS NOT NULL
      THEN
         l_column_string    := p_prefix || '.' || p_column_tab (1).column_name || CHR (10);

         FOR i IN 2 .. p_column_tab.COUNT
         LOOP
            l_column_string    := l_column_string || p_lpad || ', ' || p_prefix || '.' || p_column_tab (i).column_name || CHR (10);
         END LOOP;
      ELSE
         l_column_string    := p_column_tab (1).column_name || CHR (10);

         FOR i IN 2 .. p_column_tab.COUNT
         LOOP
            l_column_string    := l_column_string || p_lpad || ', ' || p_column_tab (i).column_name || CHR (10);
         END LOOP;
      END IF;

      l_column_string    := SUBSTR (l_column_string
                                  , 1
                                  , LENGTH (l_column_string) - 1
                                   );
      RETURN l_column_string;
   END to_string;

   FUNCTION to_string1 (
      p_column_tab   IN   column_tab_t
    , p_prefix            VARCHAR2
    , p_lpad              VARCHAR2
    , p_rpad              VARCHAR2
   )
      RETURN VARCHAR2
   IS
      l_column_string   VARCHAR2 (10000);
   BEGIN
      IF p_column_tab.COUNT = 1
      THEN
         l_column_string    := p_prefix || '.' || p_column_tab (1).column_name || ' || ''|''' || CHR (10);
      ELSE
         l_column_string    := p_prefix || '.' || p_column_tab (1).column_name || p_rpad || CHR (10);
      END IF;

      FOR i IN 2 .. p_column_tab.COUNT
      LOOP
         IF i < p_column_tab.COUNT
         THEN
            l_column_string    := l_column_string || p_lpad || p_prefix || '.' || p_column_tab (i).column_name || p_rpad || CHR (10);
         ELSE
            l_column_string    := l_column_string || p_lpad || p_prefix || '.' || p_column_tab (i).column_name || CHR (10);
         END IF;
      END LOOP;

      l_column_string    := SUBSTR (l_column_string
                                  , 1
                                  , LENGTH (l_column_string) - 1
                                   );
      RETURN l_column_string;
   END to_string1;

   FUNCTION to_string2 (
      p_column_tab   IN   column_tab_t
    , p_prefix1           VARCHAR2
    , p_prefix2           VARCHAR2
    , p_lpad              VARCHAR2
   )
      RETURN VARCHAR2
   IS
      l_column_string   VARCHAR2 (10000);
   BEGIN
      l_column_string    := p_prefix1 || '.' || p_column_tab (1).column_name || ' = ' || p_prefix2 || '.' || p_column_tab (1).column_name || CHR (10);

      FOR i IN 2 .. p_column_tab.COUNT
      LOOP
         l_column_string    := l_column_string || p_lpad || ', ' || p_prefix1 || '.' || p_column_tab (i).column_name || ' = ' || p_prefix2 || '.' || p_column_tab (i).column_name || CHR (10);
      END LOOP;

      l_column_string    := SUBSTR (l_column_string
                                  , 1
                                  , LENGTH (l_column_string) - 1
                                   );
      RETURN l_column_string;
   END to_string2;

   FUNCTION init_nk_column_tab (
      p_tgt_table_name   IN   VARCHAR2
    , p_nk_column_list   IN   VARCHAR2 DEFAULT NULL
   )
      RETURN column_tab_t
   IS
      l_column_tab       column_tab_t;
      l_nk_column_list   VARCHAR2 (300);
   BEGIN
      IF p_nk_column_list IS NULL
      THEN
         SELECT column_id
              , table_name
              , column_name
              , data_type
              , CASE
                   WHEN char_length = 0
                      THEN data_length
                   ELSE char_length
                END data_length
              , data_precision
              , data_scale
              , nullable
         BULK COLLECT INTO l_column_tab
           -- toDo  > remove DB-Link
         FROM   all_tab_columns
          WHERE owner = c_core_user
            AND table_name = p_tgt_table_name
            AND column_name LIKE '%\_NK' ESCAPE '\';
      ELSE
         l_nk_column_list    := RTRIM (p_nk_column_list, c_param_delimiter);

         SELECT NULL
              , NULL
              , a.column_name
              , NULL
              , NULL
              , NULL
              , NULL
              , NULL
         BULK COLLECT INTO l_column_tab
           FROM (SELECT     REGEXP_SUBSTR (l_nk_column_list
                                         , '[^' || c_param_delimiter || ']+'
                                         , 1
                                         , ROWNUM
                                          ) column_name
                       FROM DUAL
                 CONNECT BY LEVEL <= LENGTH (REGEXP_REPLACE (l_nk_column_list, '[^' || c_param_delimiter || ']+')) + 1) a;
      END IF;

      RETURN l_column_tab;
   END init_nk_column_tab;

   FUNCTION get_tgt_dummy_column_tab (
      p_tgt_table_name   IN   VARCHAR2
   )
      RETURN column_tab_t
   IS
      l_column_tab   column_tab_t;
   BEGIN
      SELECT column_id
           , table_name
           , column_name
           , data_type
           , CASE
                WHEN char_length = 0
                   THEN data_length
                ELSE char_length
             END data_length
           , data_precision
           , data_scale
           , nullable
      BULK COLLECT INTO l_column_tab
        -- toDo  > remove DB-Link
      FROM   all_tab_columns
       WHERE owner = c_core_user
         AND table_name = p_tgt_table_name
         AND column_name NOT IN (
                                 -- PK Spalten in Zieltabelle  ausschliessen
                                 SELECT b.column_name
                                   -- toDo  > remove DB-Link
                                 FROM   all_constraints a
                                      , all_ind_columns b
                                  WHERE a.owner = c_core_user
                                    AND a.table_name = p_tgt_table_name
                                    AND a.constraint_type = 'P'
                                    AND b.table_owner = c_core_user
                                    AND a.index_name = b.index_name)
         -- technische Spalten  in Zieltabelle  ausschliessen
         AND column_name NOT IN ('DI_GUI', 'DI_GUI_INS', 'DI_SYSTEM')
         -- NK Spalten  in Zieltabelle  ausschliessen
         AND column_name NOT LIKE '%\_NK' ESCAPE '\';

      RETURN l_column_tab;
   END get_tgt_dummy_column_tab;

   FUNCTION get_hist_column_tab (
      p_tgt_table_name   IN   VARCHAR2
   )
      RETURN column_tab_t
   IS
      l_column_tab   column_tab_t;
   BEGIN
      SELECT column_id
           , table_name
           , column_name
           , data_type
           , CASE
                WHEN char_length = 0
                   THEN data_length
                ELSE char_length
             END data_length
           , data_precision
           , data_scale
           , nullable
      BULK COLLECT INTO l_column_tab
        -- toDo  > remove DB-Link
      FROM   all_tab_columns
       WHERE owner = c_core_user
         AND table_name = p_tgt_table_name
         AND column_name NOT IN (
                                 -- PK Spalten in Zieltabelle  ausschliessen
                                 SELECT b.column_name
                                   -- toDo  > remove DB-Link
                                 FROM   all_constraints a
                                      , all_ind_columns b
                                  WHERE a.owner = c_core_user
                                    AND a.table_name = p_tgt_table_name
                                    AND a.constraint_type = 'P'
                                    AND b.table_owner = c_core_user
                                    AND a.index_name = b.index_name)
         -- technische Spalten  in Zieltabelle  ausschliessen
         AND column_name NOT IN ('DI_GUI', 'DI_GUI_INS', 'ACTIVE', 'VALID_FROM', 'VALID_TO');

      RETURN l_column_tab;
   END get_hist_column_tab;

   FUNCTION master_propagation_needed (
      p_src_table_name      IN   VARCHAR2
    , p_tgt_table_name      IN   VARCHAR2
    , p_src_nk_column_tab   IN   column_tab_t
    , p_tgt_nk_column_tab   IN   column_tab_t
   )
      RETURN BOOLEAN
   IS
      l_sql   VARCHAR2 (32767);
      l_cnt   NUMBER;
   BEGIN
      l_sql    :=
            ' 
select --+ ordered use_hash (src tgt) full(src) parallel(src 3) 
       count(*)
  from '
         || p_src_table_name
         || c_core_db_link
         || ' src
     , '
         || c_core_user
         || '.'
         || p_tgt_table_name
         || c_core_db_link
         || ' tgt
 -- Antijoin source vs. target
 where src.';
      l_sql    := l_sql || p_src_nk_column_tab (1).column_name || ' IS NOT NULL ';

      FOR i IN 2 .. p_src_nk_column_tab.COUNT
      LOOP
         l_sql    := l_sql || ' 
   and src.' ||      p_src_nk_column_tab (i).column_name || ' IS NOT NULL ';
      END LOOP;

      FOR i IN 1 .. p_src_nk_column_tab.COUNT
      LOOP
         l_sql    := l_sql || ' 
   and src.' ||      p_src_nk_column_tab (i).column_name || ' = tgt.' || p_tgt_nk_column_tab (i).column_name || ' (+)';
      END LOOP;

      l_sql    := l_sql || ' 
   and tgt.' ||   p_tgt_nk_column_tab (1).column_name || ' IS NULL 
';

      EXECUTE IMMEDIATE l_sql
                   INTO l_cnt;

      IF l_cnt > 0
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END master_propagation_needed;

---------------------------------------------------
-- PROCEDURES
---------------------------------------------------
   PROCEDURE write_statistics (
      p_tgt_tab              IN   VARCHAR
    , p_di_system            IN   VARCHAR
    , p_di_gui               IN   NUMBER
    , p_rows_ins             IN   NUMBER
    , p_rows_upd             IN   NUMBER
    , p_rows_upd_versioned   IN   NUMBER
    , p_rows_del             IN   NUMBER
    , p_rows_del_versioned   IN   NUMBER
   )
   IS
   BEGIN
      INSERT INTO utl_load_statistics_t
                  (di_tgt_table_name
                 , di_system
                 , di_gui
                 , rows_ins
                 , rows_upd
                 , rows_upd_versioned
                 , rows_del
                 , rows_del_versioned
                 , create_dt
                 , changed_dt
                  )
           VALUES (p_tgt_tab
                 , p_di_system
                 , p_di_gui
                 , p_rows_ins
                 , p_rows_upd
                 , p_rows_upd_versioned
                 , p_rows_del
                 , p_rows_del_versioned
                 , SYSDATE
                 , SYSDATE
                  );

      COMMIT;
   END write_statistics;

   PROCEDURE print_column_list (
      p_column_tab   IN   column_tab_t
   )
   IS
   BEGIN
      log.LOG ('---------------------------------------------');

      FOR i IN 1 .. p_column_tab.COUNT
      LOOP
         log.LOG (i || ': ' || p_column_tab (i).column_name);
      END LOOP;

      log.LOG ('---------------------------------------------');
      log.LOG (p_column_tab.COUNT || ' records in Column List.');
   END print_column_list;

   PROCEDURE gen_sql_insert_master (
      p_src_table_name         IN       VARCHAR2
    , p_tgt_table_name         IN       VARCHAR2
    , p_src_nk_column_tab      IN       column_tab_t
    , p_tgt_nk_column_tab      IN       column_tab_t
    , p_tgt_dummy_column_tab   IN       column_tab_t
    , p_di_gui                 IN       NUMBER
    , p_sql                    OUT      VARCHAR2
   )
   IS
      l_sql   VARCHAR2 (32767);
   BEGIN
      l_sql    :=
            ' 
insert into '
         || c_core_user
         || '.'
         || p_tgt_table_name
         || c_core_db_link
         || '
(
  '
         || p_tgt_table_name
         || '_ID
, DI_GUI
, DI_GUI_INS
, DI_SYSTEM
, '
         || to_string (p_tgt_nk_column_tab)
         || '
)
select '
         || c_core_user
         || '.S_'
         || p_tgt_table_name
         || '_ID.NEXTVAL'
         || c_core_db_link
         || '
     , DI_GUI
     , DI_GUI_INS
     , DI_SYSTEM
     , '
         || to_string (p_src_nk_column_tab
                     , NULL
                     , '     '
                      )
         || '
  from (select distinct  
               --+ ordered use_hash (src tgt) full(src) parallel(src 3) index(tgt)
               '
         || p_di_gui
         || ' as DI_GUI
             , '
         || p_di_gui
         || ' as DI_GUI_INS
             , ''from '
         || p_src_table_name
         || ''' as DI_SYSTEM
             , '
         || to_string (p_src_nk_column_tab
                     , 'src'
                     , '             '
                      )
         || '
          from '
         || p_src_table_name
         || c_core_db_link
         || ' src
             , '
         || c_core_user
         || '.'
         || p_tgt_table_name
         || c_core_db_link
         || ' tgt
         -- Antijoin source vs. target
         where src.';
      l_sql    := l_sql || p_src_nk_column_tab (1).column_name || ' IS NOT NULL ';

      FOR i IN 2 .. p_src_nk_column_tab.COUNT
      LOOP
         l_sql    := l_sql || ' 
           and src.' || p_src_nk_column_tab (i).column_name || ' IS NOT NULL ';
      END LOOP;

      FOR i IN 1 .. p_src_nk_column_tab.COUNT
      LOOP
         l_sql    := l_sql || ' 
           and src.' || p_src_nk_column_tab (i).column_name || ' = tgt.' || p_tgt_nk_column_tab (i).column_name || ' (+)';
      END LOOP;

      l_sql    := l_sql || ' 
           and tgt.' || p_tgt_nk_column_tab (1).column_name || ' IS NULL 
       )';
      p_sql    := l_sql;
   END gen_sql_insert_master;

   PROCEDURE gen_sql_insert_master_hist (
      p_o_table_name          IN       VARCHAR2
    , p_src_hist_column_tab   IN       column_tab_t
    , p_di_gui                IN       NUMBER
    , p_sql                   OUT      VARCHAR2
   )
   IS
      l_sql   VARCHAR2 (32767);
   BEGIN
      l_sql    :=
            '
insert into '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '
(
  '
         || p_o_table_name
         || '_SK 
, DI_GUI  
, DI_GUI_INS
, ACTIVE
, VALID_FROM
, VALID_TO
, '
         || to_string (p_src_hist_column_tab)
         || '
)
select --+ ordered parallel (src 3) index(tgt)
       '
         || c_core_user
         || '.S_'
         || p_o_table_name
         || '_SK.NEXTVAL'
         || c_core_db_link
         || '
     , '
         || p_di_gui
         || ' as DI_GUI
     , '
         || p_di_gui
         || ' as DI_GUI_INS
     , 1  as ACTIVE
     , to_date( ''11.11.1111'',''DD.MM.YYYY'')  as VALID_FROM
     , to_date( ''09.09.9999'',''DD.MM.YYYY'')  as VALID_TO
     , '
         || to_string (p_src_hist_column_tab
                     , 'src'
                     , '     '
                      )
         || '
  from '
         || c_core_user
         || '.'
         || p_o_table_name
         || c_core_db_link
         || ' src
     , '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '     tgt
 -- Antijoin source vs. target
 -- historization only for current di_gui
 where src.DI_GUI = '
         || p_di_gui
         || '
   and src.'
         || p_o_table_name
         || '_ID  =  tgt.'
         || p_o_table_name
         || '_ID (+)
   and tgt.'
         || p_o_table_name
         || '_ID   IS NULL
';
      p_sql    := l_sql;
   END gen_sql_insert_master_hist;

   PROCEDURE gen_sql_insert_hist (
      p_o_table_name          IN       VARCHAR2
    , p_src_hist_column_tab   IN       column_tab_t
    , p_di_gui                IN       NUMBER
    , p_max_di_gui_hist       IN       NUMBER
    , p_sql                   OUT      VARCHAR2
   )
   IS
      l_sql   VARCHAR2 (32767);
   BEGIN
      l_sql    :=
            '
insert into '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '
(
  '
         || p_o_table_name
         || '_SK 
, DI_GUI  
, DI_GUI_INS
, ACTIVE
, VALID_FROM
, VALID_TO
, '
         || to_string (p_src_hist_column_tab)
         || '
)
select --+ ordered parallel (src 3) index(tgt)
       '
         || c_core_user
         || '.S_'
         || p_o_table_name
         || '_SK.NEXTVAL'
         || c_core_db_link
         || '
     , '
         || p_di_gui
         || ' as DI_GUI
     , '
         || p_di_gui
         || ' as DI_GUI_INS
     , 1  as ACTIVE
     , to_date( ''11.11.1111'',''DD.MM.YYYY'')  as VALID_FROM
     , to_date( ''09.09.9999'',''DD.MM.YYYY'')  as VALID_TO
     , '
         || to_string (p_src_hist_column_tab
                     , 'src'
                     , '     '
                      )
         || '
  from '
         || c_core_user
         || '.'
         || p_o_table_name
         || c_core_db_link
         || ' src
     , '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '     tgt
 -- Antijoin source vs. target
 where src.DI_GUI > '
         || p_max_di_gui_hist
         || '
   and src.'
         || p_o_table_name
         || '_ID  =  tgt.'
         || p_o_table_name
         || '_ID (+)
   and tgt.'
         || p_o_table_name
         || '_ID   IS NULL
';
      p_sql    := l_sql;
   END gen_sql_insert_hist;

   PROCEDURE gen_sql_update_hist_versioned (
      p_o_table_name      IN       VARCHAR2
    , p_hist_column_tab   IN       column_tab_t
    , p_di_gui            IN       NUMBER
    , p_max_di_gui_hist   IN       NUMBER
    , p_cutoff_day        IN       VARCHAR2
    , p_sql               OUT      VARCHAR2
   )
   IS
      l_sql                  VARCHAR2 (32767);
      l_reduced_column_tab   column_tab_t;
      j                      NUMBER;
   BEGIN
      l_reduced_column_tab    := column_tab_t ();
      j                       := 0;

      FOR i IN 1 .. p_hist_column_tab.COUNT
      LOOP
         IF p_hist_column_tab (i).column_name != p_o_table_name || '_ID'
         THEN
            j                           := j + 1;
            l_reduced_column_tab.EXTEND;
            l_reduced_column_tab (j)    := p_hist_column_tab (i);
         END IF;
      END LOOP;

      l_sql                   :=
            '
update --+ ROWID(tgt)
       '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '
   set ACTIVE   = 0
     , VALID_TO = to_date('''
         || p_cutoff_day
         || ''', ''DD.MM.YYYY'')
     , DI_GUI = '
         || p_di_gui
         || '
where ROWID in
(select rowid 
   from (select --+ ordered parallel (src 3) parallel (tgt 3 )
               tgt.rowid
          from '
         || c_core_user
         || '.'
         || p_o_table_name
         || c_core_db_link
         || ' src,
               '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || ' tgt          
         where src.DI_GUI > '
         || p_max_di_gui_hist
         || '
           and src.'
         || p_o_table_name
         || '_ID  =  tgt.'
         || p_o_table_name
         || '_ID 
           and tgt.ACTIVE = 1
           and tgt.VALID_FROM < to_date('''
         || p_cutoff_day
         || ''', ''DD.MM.YYYY'')
           and '
         || to_string1 (l_reduced_column_tab
                      , 'src'
                      , '               '
                      , ' || ''|'' ||'
                       )
         || '
            != '
         || to_string1 (l_reduced_column_tab
                      , 'tgt'
                      , '               '
                      , ' || ''|'' ||'
                       )
         || ' 
         order by tgt.rowid  
        ) src
)
';
      p_sql                   := l_sql;
   END gen_sql_update_hist_versioned;

   PROCEDURE gen_sql_update_hist (
      p_o_table_name      IN       VARCHAR2
    , p_hist_column_tab   IN       column_tab_t
    , p_di_gui            IN       NUMBER
    , p_max_di_gui_hist   IN       NUMBER
    , p_cutoff_day        IN       VARCHAR2
    , p_sql               OUT      VARCHAR2
   )
   IS
      l_sql                  VARCHAR2 (32767);
      l_reduced_column_tab   column_tab_t;
      j                      NUMBER;
   BEGIN
      l_reduced_column_tab    := column_tab_t ();
      j                       := 0;

      FOR i IN 1 .. p_hist_column_tab.COUNT
      LOOP
         IF p_hist_column_tab (i).column_name != p_o_table_name || '_ID'
         THEN
            j                           := j + 1;
            l_reduced_column_tab.EXTEND;
            l_reduced_column_tab (j)    := p_hist_column_tab (i);
         END IF;
      END LOOP;

--   print_column_list(l_reduced_column_tab);
      l_sql                   :=
            '
merge  --+ rowid (tgt)    
 into '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || ' tgt  
using
( select --+ ordered parallel (src 3) parallel (tgt 3 ) 
         tgt.ROWID 
       , src.DI_GUI
       , '
         || to_string (p_hist_column_tab
                     , 'src'
                     , '       '
                      )
         || '
    from '
         || c_core_user
         || '.'
         || p_o_table_name
         || c_core_db_link
         || ' src,
         '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || ' tgt          
   where src.DI_GUI > '
         || p_max_di_gui_hist
         || '
     and src.'
         || p_o_table_name
         || '_ID  =  tgt.'
         || p_o_table_name
         || '_ID 
     and tgt.ACTIVE = 1
     and tgt.VALID_FROM = to_date('''
         || p_cutoff_day
         || ''', ''DD.MM.YYYY'')
     and '
         || to_string1 (l_reduced_column_tab
                      , 'src'
                      , '         '
                      , ' || ''|'' ||'
                       )
         || '
      != '
         || to_string1 (l_reduced_column_tab
                      , 'tgt'
                      , '         '
                      , ' || ''|'' ||'
                       )
         || ' 
   order by tgt.rowid  
) sub
ON (tgt.rowid = sub.rowid)
WHEN MATCHED THEN UPDATE SET 
  tgt.DI_GUI = '
         || p_di_gui
         || '
, '
         || to_string2 (l_reduced_column_tab
                      , 'tgt'
                      , 'sub'
                      , ''
                       )
         || '
';
      p_sql                   := l_sql;
   END gen_sql_update_hist;

   PROCEDURE gen_sql_insert_hist_versioned (
      p_o_table_name      IN       VARCHAR2
    , p_hist_column_tab   IN       column_tab_t
    , p_di_gui            IN       NUMBER
    , p_max_di_gui_hist   IN       NUMBER
    , p_cutoff_day        IN       VARCHAR2
    , p_sql               OUT      VARCHAR2
   )
   IS
      l_sql   VARCHAR2 (32767);
   BEGIN
      l_sql    :=
            '
insert into '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || '
(
  '
         || p_o_table_name
         || '_SK 
, DI_GUI
, DI_GUI_INS
, ACTIVE
, VALID_FROM
, VALID_TO
, '
         || to_string (p_hist_column_tab)
         || '
)
select --+ ordered parallel (src 3) parallel (tgt 3) 
       '
         || c_core_user
         || '.S_'
         || p_o_table_name
         || '_SK.NEXTVAL'
         || c_core_db_link
         || '
     , '
         || p_di_gui
         || ' as DI_GUI
     , '
         || p_di_gui
         || ' as DI_GUI_INS
     , 1  as ACTIVE
     , to_date('''
         || p_cutoff_day
         || ''', ''DD.MM.YYYY'')  as VALID_FROM
     , to_date( ''09.09.9999'',''DD.MM.YYYY'')  as VALID_TO
     , '
         || to_string (p_hist_column_tab
                     , 'src'
                     , '     '
                      )
         || '
  from '
         || c_core_user
         || '.'
         || p_o_table_name
         || c_core_db_link
         || ' src,
       '
         || c_core_user
         || '.'
         || p_o_table_name
         || '_H'
         || c_core_db_link
         || ' tgt
 where src.DI_GUI > '
         || p_max_di_gui_hist
         || '
   and src.'
         || p_o_table_name
         || '_ID  =  tgt.'
         || p_o_table_name
         || '_ID 
   and tgt.DI_GUI = '
         || p_di_gui
         || '
     and tgt.VALID_TO = to_date('''
         || p_cutoff_day
         || ''', ''DD.MM.YYYY'')
   and tgt.ACTIVE = 0
';
      p_sql    := l_sql;
   END gen_sql_insert_hist_versioned;

---------------------------------------------------
-- PUBLIC FUNCTIONS
---------------------------------------------------
   FUNCTION get_sequence (
      p_sequence_name   IN   VARCHAR2
    , p_sequence_val    IN   NUMBER
   )
      RETURN NUMBER
   IS
      l_seq_num   NUMBER;
   BEGIN
      IF p_sequence_val IS NOT NULL
      THEN
         RETURN p_sequence_val;
      ELSE
--        execute immediate 'select count(1) from ' || tbl || ' where ' || attr || ' = :a' into cnt using attrval;
--        EXECUTE IMMEDIATE 'SELECT EDWH_CORE.S_SUB_ACCOUNT_ID.NEXTVAL@EDWH_CL@CORE from  dual'  into  l_seq_num;
         EXECUTE IMMEDIATE 'SELECT ' || p_sequence_name || ' from  dual'
                      INTO l_seq_num;

--        SELECT EDWH_CORE.S_SUB_ACCOUNT_ID.NEXTVAL@EDWH_CL@CORE
--        select EDWH_CL.SEQ_ETL_TEST.nextval
         RETURN l_seq_num;
      END IF;
   END get_sequence;

   FUNCTION fill_master_tab_multi_nk (
      p_job_name             IN       VARCHAR2
    , p_workflow_name        IN       VARCHAR2
    , p_di_gui               IN       NUMBER
    , p_src_table_name       IN       VARCHAR2
    , p_tgt_table_name       IN       VARCHAR2
    , p_src_nk_column_list   IN       VARCHAR2
    , p_tgt_nk_column_list   IN       VARCHAR2
    , p_log_message          OUT      VARCHAR2
    , p_do_not_execute       IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER
   IS
      -- Konstante für Procedure-Name
      c_proc_name          CONSTANT VARCHAR2 (35)    := UPPER ('fill_master_tab_multi_nk ');
      l_max_di_gui_hist             NUMBER;
      l_max_valid_from_hist         DATE;
      l_sql                         VARCHAR2 (32767);
      l_sql_h                       VARCHAR2 (32767);
      l_row_cnt                     NUMBER;
      l_row_cnt_h                   NUMBER;
      l_tgt_dummy_column_list       VARCHAR2 (32767);
      l_src_nk_column_tab           column_tab_t;
      l_tgt_nk_column_tab           column_tab_t;
      l_tgt_dummy_column_tab        column_tab_t;
      l_hist_column_tab             column_tab_t;
      l_cls_table_exists            BOOLEAN;
      l_master_table_exists         BOOLEAN;
      l_history_table_exists        BOOLEAN;
      l_nk_columns_ok               BOOLEAN;
      l_master_propagation_needed   BOOLEAN;
      l_src_table_name              VARCHAR2 (50);
      l_tgt_table_name              VARCHAR2 (50);
      l_src_nk_column_list          VARCHAR2 (400);
      l_tgt_nk_column_list          VARCHAR2 (400);
   BEGIN
      DBMS_APPLICATION_INFO.set_module (c_package_name, c_proc_name);
      DBMS_APPLICATION_INFO.set_client_info (p_job_name || ': ' || c_proc_name);
      DBMS_APPLICATION_INFO.set_action ('started');

      IF c_hash_area != 0
      THEN
         EXECUTE IMMEDIATE 'ALTER SESSION SET HASH_AREA_SIZE=' || c_hash_area;

         EXECUTE IMMEDIATE 'ALTER SESSION SET SORT_AREA_SIZE=' || c_hash_area / 2;
      END IF;

      EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

      l_src_table_name               := UPPER (p_src_table_name);
      l_tgt_table_name               := UPPER (p_tgt_table_name);
      l_src_nk_column_list           := UPPER (p_src_nk_column_list);
      l_tgt_nk_column_list           := UPPER (p_tgt_nk_column_list);
      l_max_di_gui_hist              := 0;
      l_max_valid_from_hist          := TO_DATE ('11.11.1111', 'DD.MM.YYYY');
      log.set_di_gui ((p_di_gui));
      log.set_job_name (p_job_name);
      log.LOG ('Fill Master Table ' || l_tgt_table_name || ' (from ' || l_src_table_name || ')', 'START');

      -- Check Parameter p_di_gui
      IF NVL (p_di_gui, 0) <= 0
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Parameter DI_GUI is invalid (p_di_gui=' || p_di_gui || ')';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      -- Check NK Parameter
      IF    (    p_src_nk_column_list IS NULL
             AND p_tgt_nk_column_list IS NOT NULL)
         OR (    p_tgt_nk_column_list IS NULL
             AND p_src_nk_column_list IS NOT NULL)
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'One NK-Parameter is NULL (should be both either "null" or "not null"';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      l_cls_table_exists             := cls_table_exists (l_src_table_name);
      l_master_table_exists          := core_table_exists (l_tgt_table_name);
      l_history_table_exists         := core_table_exists (l_tgt_table_name || '_H');

      IF NOT l_cls_table_exists
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'CLS Table ' || l_src_table_name || ' does not extist.';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      IF NOT l_master_table_exists
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Master Table ' || l_tgt_table_name || ' does not extist.';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      -- SOURCE (CLS Detail Table)
      -- fill list with NK source columns
      l_src_nk_column_tab            := init_nk_column_tab (l_tgt_table_name, l_src_nk_column_list);
--    log.LOG ('Printing SRC NK Column List:' );
--    print_column_list(l_src_nk_column_tab);

      -- fill list with NK target columns
      l_tgt_nk_column_tab            := init_nk_column_tab (l_tgt_table_name, l_tgt_nk_column_list);
--    log.LOG ('Printing TGT NK Column List:' );
--    print_column_list(l_tgt_nk_column_tab);

      -- fill list with dummy target columns (no PK and no NK columns)
      l_tgt_dummy_column_tab         := get_tgt_dummy_column_tab (l_tgt_table_name);
--    log.LOG ('Printing TGT Dummy Column List:' );
--    print_column_list(l_tgt_dummy_column_tab);

      -- validate  NK Columns: (number of parameters must match, data type must match, name mut be '%_NK')
      l_nk_columns_ok                := validate_nk_columns (l_src_table_name
                                                           , l_tgt_table_name
                                                           , l_src_nk_column_tab
                                                           , l_tgt_nk_column_tab
                                                           , p_log_message
                                                            );

      IF NOT l_nk_columns_ok
      THEN
         log.set_console_logging (TRUE);
         log.LOG (p_log_message, 'ERROR');
         p_log_message    := p_log_message || CHR (10) || 'Problems with Natural Key Columns detected.';
         log.LOG ('Problems with Natural Key Columns detected.', 'ERROR');
         RETURN g_ret_nok;
      END IF;

      l_master_propagation_needed    := master_propagation_needed (l_src_table_name
                                                                 , l_tgt_table_name
                                                                 , l_src_nk_column_tab
                                                                 , l_tgt_nk_column_tab
                                                                  );

      IF NOT l_master_propagation_needed
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'NO NK propagation into Table ' || l_tgt_table_name || ' (from ' || l_src_table_name || ') needed.';
         log.LOG (p_log_message, 'END');
         RETURN g_ret_ok;
      END IF;

      BEGIN
         EXECUTE IMMEDIATE 'LOCK TABLE ' || c_core_user || '.' || l_tgt_table_name || ' IN EXCLUSIVE MODE NOWAIT';

         log.LOG ('Locked Table ' || c_core_user || '.' || l_tgt_table_name, 'LOCK TABLE');
      EXCEPTION
         -- object statistics are locked
         WHEN e_lock_detected
         THEN
            log.set_console_logging (TRUE);
            p_log_message    := 'Could not lock Table ' || l_tgt_table_name;
            log.LOG (p_log_message, 'LOCK TABLE');
            RETURN g_ret_no_table_lock;
         WHEN OTHERS
         THEN
            RAISE;
      END;

      IF l_history_table_exists
      THEN
         l_max_di_gui_hist    := get_max_di_gui (l_tgt_table_name || '_H', l_max_valid_from_hist);
         -- fill list with hist columns (no PK and no technichal columns)
         l_hist_column_tab    := get_hist_column_tab (l_tgt_table_name || '_H');
--      log.LOG ('Printing HIST Column List:' );
--      print_column_list(l_hist_column_tab);
      END IF;

      --  generate SQL Statement for insert into Master Table:
      --  l_sql returns the result;
      gen_sql_insert_master (l_src_table_name
                           , l_tgt_table_name
                           , l_src_nk_column_tab
                           , l_tgt_nk_column_tab
                           , l_tgt_dummy_column_tab
                           , p_di_gui
                           , l_sql
                            );

      IF l_history_table_exists
      THEN
         --  generate SQL Statement for insert into Master History Table:
         --  l_sql returns the result;
         gen_sql_insert_master_hist (l_tgt_table_name
                                   , l_hist_column_tab
                                   , p_di_gui
                                   , l_sql_h
                                    );
      END IF;

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG (l_sql);
         log.LOG (l_sql_h);
         p_log_message    := 'Genrated SQL Statement  for Master Table (do_not_execute = Y):';
         p_log_message    := p_log_message || l_sql;
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Inserting into Table' || c_core_user || '.' || l_tgt_table_name);
         log.LOG ('Inserting into Master Table ' || c_core_user || '.' || l_tgt_table_name);
         COMMIT;

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt        := SQL%ROWCOUNT;

         IF l_history_table_exists
         THEN
            DBMS_APPLICATION_INFO.set_action ('Inserting into Table' || c_core_user || '.' || l_tgt_table_name || '_H');
            log.LOG ('Inserting into Master History Table ' || c_core_user || '.' || l_tgt_table_name || '_H');
            COMMIT;

            EXECUTE IMMEDIATE l_sql_h;

            l_row_cnt_h    := SQL%ROWCOUNT;
         END IF;

         COMMIT;
         p_log_message    := CHR (10) || 'Inserted ' || l_row_cnt || ' rows into Master         Table ' || l_tgt_table_name || '    (from  CLS Detail Table ' || l_src_table_name || ')' || CHR (10);
         write_statistics (l_tgt_table_name
                         , 'from ' || l_src_table_name
                         , p_di_gui
                         , l_row_cnt
                         , 0
                         , 0
                         , 0
                         , 0
                          );

         IF l_history_table_exists
         THEN
            p_log_message    := p_log_message || 'Inserted ' || l_row_cnt_h || ' rows into Master History Table ' || l_tgt_table_name || '_H';
            write_statistics (l_tgt_table_name || '_H'
                            , 'from ' || l_src_table_name
                            , p_di_gui
                            , l_row_cnt_h
                            , 0
                            , 0
                            , 0
                            , 0
                             );
         END IF;

         log.LOG (p_log_message);
      END IF;

      log.LOG ('Fill Master Table ' || l_tgt_table_name || ' (from ' || l_src_table_name || ')', 'END');
      RETURN g_ret_ok;
   EXCEPTION
      WHEN OTHERS
      THEN
         log.set_console_logging (TRUE);
         log.LOG (SQLERRM
                        , log.gc_fatal
                        , SQLCODE
                         );
         log.LOG ('Fill Master Table ' || l_tgt_table_name || ' (from ' || l_src_table_name || ')', 'ERROR');
         ROLLBACK;
         RAISE;
   END fill_master_tab_multi_nk;

   FUNCTION fill_master_tab (
      p_job_name         IN       VARCHAR2
    , p_workflow_name    IN       VARCHAR2
    , p_di_gui           IN       NUMBER
    , p_src_table_name   IN       VARCHAR2
    , p_tgt_table_name   IN       VARCHAR2
    , p_log_message      OUT      VARCHAR2
    , p_do_not_execute   IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER
   IS
      -- Konstante für Procedure-Name
      c_proc_name   CONSTANT VARCHAR2 (35) := UPPER ('fill_master_tab');
      l_ret_val              NUMBER;
   BEGIN
      DBMS_APPLICATION_INFO.set_module (c_package_name, c_proc_name);
      DBMS_APPLICATION_INFO.set_client_info (p_job_name || ': ' || c_proc_name);
      DBMS_APPLICATION_INFO.set_action ('started');
      l_ret_val    := fill_master_tab_multi_nk (p_job_name
                                              , p_workflow_name
                                              , p_di_gui
                                              , p_src_table_name
                                              , p_tgt_table_name
                                              , NULL
                                              , NULL
                                              , p_log_message
                                              , p_do_not_execute
                                               );
      RETURN l_ret_val;
   EXCEPTION
      WHEN OTHERS
      THEN
         log.set_console_logging (TRUE);
         log.LOG (SQLERRM
                        , log.gc_fatal
                        , SQLCODE
                         );
         log.LOG ('Fill Master Table ' || p_tgt_table_name || ' (from ' || p_src_table_name || ')', 'ERROR');
         ROLLBACK;
         RAISE;
   END fill_master_tab;

   FUNCTION fill_hist_tab (
      p_job_name          IN       VARCHAR2
    , p_workflow_name     IN       VARCHAR2
    , p_di_gui            IN       NUMBER
    , p_hist_table_name   IN       VARCHAR2
    , p_log_message       OUT      VARCHAR2
    , p_cutoff_day                 DATE DEFAULT NULL
    , p_do_not_execute    IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER
   IS
      -- Konstante für Procedure-Name
      c_proc_name        CONSTANT VARCHAR2 (35)    := UPPER ('fill_hist_tab');
      l_cutoff_day                DATE;
      l_max_di_gui_hist           NUMBER;
      l_max_valid_from_hist       DATE;
      l_o_table_name              VARCHAR2 (35);
      l_hist_table_name           VARCHAR2 (35);
      l_o_table_exists            BOOLEAN;
      l_hist_table_exists         BOOLEAN;
      l_hist_column_tab           column_tab_t;
      l_sql_ins                   VARCHAR2 (32767);
      l_sql_upd_versioned         VARCHAR2 (32767);
      l_sql_upd                   VARCHAR2 (32767);
      l_sql_ins_versioned         VARCHAR2 (32767);
      l_inserted_rows             NUMBER;
      l_updated_rows              NUMBER;
      l_inserted_rows_versioned   NUMBER;
      l_updated_rows_versioned    NUMBER;
   BEGIN
      DBMS_APPLICATION_INFO.set_module (c_package_name, c_proc_name);
      DBMS_APPLICATION_INFO.set_client_info (p_job_name || ': ' || c_proc_name);
      DBMS_APPLICATION_INFO.set_action ('started');

      IF c_hash_area != 0
      THEN
         EXECUTE IMMEDIATE 'ALTER SESSION SET HASH_AREA_SIZE=' || c_hash_area;

         EXECUTE IMMEDIATE 'ALTER SESSION SET SORT_AREA_SIZE=' || c_hash_area / 2;
      END IF;

      EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

      log.set_di_gui ((p_di_gui));
      log.set_job_name (p_job_name);
      log.LOG ('Fill History Table ' || p_hist_table_name, 'START');
      l_inserted_rows              := 0;
      l_updated_rows               := 0;
      l_inserted_rows_versioned    := 0;
      l_updated_rows_versioned     := 0;
      l_cutoff_day                 := TRUNC (NVL (p_cutoff_day, SYSDATE));

      IF l_cutoff_day > SYSDATE
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Parameter CUTOFF_DAY is in the future (p_cutoff_day=' || l_cutoff_day || ')';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      l_max_di_gui_hist            := 0;
      l_hist_table_name            := UPPER (p_hist_table_name);
      l_o_table_name               := SUBSTR (l_hist_table_name
                                            , 1
                                            , LENGTH (l_hist_table_name) - 2
                                             );
      l_hist_table_exists          := core_table_exists (l_hist_table_name);
      l_o_table_exists             := core_table_exists (l_o_table_name);

      IF NVL (p_di_gui, 0) <= 0
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Parameter DI_GUI is invalid (p_di_gui=' || p_di_gui || ')';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      IF NOT l_hist_table_exists
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'History Table ' || l_hist_table_name || ' does not extist.';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      IF NOT l_o_table_exists
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Table ' || l_o_table_name || ' does not extist.';
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      BEGIN
         EXECUTE IMMEDIATE 'LOCK TABLE ' || c_core_user || '.' || l_o_table_name || ' IN EXCLUSIVE MODE NOWAIT';

         log.LOG ('Locked Table ' || c_core_user || '.' || l_o_table_name, 'LOCK TABLE');
      EXCEPTION
         -- object statistics are locked
         WHEN e_lock_detected
         THEN
            log.set_console_logging (TRUE);
            p_log_message    := 'Could not lock Table ' || l_o_table_name;
            log.LOG (p_log_message, 'LOCK TABLE');
            RETURN g_ret_no_table_lock;
         WHEN OTHERS
         THEN
            RAISE;
      END;

      -- get max di_gui from hist table
      l_max_di_gui_hist            := get_max_di_gui (l_hist_table_name, l_max_valid_from_hist);

      IF l_max_valid_from_hist > l_cutoff_day
      THEN
         log.set_console_logging (TRUE);
         p_log_message    := 'Parameter CUTOFF_DAY is too old (p_cutoff_day=' || TO_CHAR (p_cutoff_day, 'DD.MM.YYYY') || ')' || CHR (10);
         p_log_message    := p_log_message || 'max(valid_from) in table ' || c_core_user || '.' || l_hist_table_name || ' is : ' || TO_CHAR (l_max_valid_from_hist, 'DD.MM.YYYY');
         log.LOG (p_log_message, 'ERROR');
         RETURN g_ret_nok;
      END IF;

      log.LOG ('CUTOFF_DAY is : ' || TO_CHAR (l_cutoff_day, 'DD.MM.YYYY'));
      -- fill list with hist columns (no PK and no technichal columns)
      l_hist_column_tab            := get_hist_column_tab (l_hist_table_name);
      --  generate SQL Statement for inserting time intervals
      --  for new entities into  History Table:
      --  l_sql_ins returns the result;
      gen_sql_insert_hist (l_o_table_name
                         , l_hist_column_tab
                         , p_di_gui
                         , l_max_di_gui_hist
                         , l_sql_ins
                          );

      IF l_hist_column_tab.COUNT > 1
      THEN
         --  generate SQL Statement which closes time intervals
         --  for changed entities:
         --  l_sql_upd_versioned the result;
         gen_sql_update_hist_versioned (l_o_table_name
                                      , l_hist_column_tab
                                      , p_di_gui
                                      , l_max_di_gui_hist
                                      , TO_CHAR (l_cutoff_day, 'DD.MM.YYYY')
                                      , l_sql_upd_versioned
                                       );
         --  generate SQL Statement which updates (inplace = unversioned) time intervals
         --  l_sql_upd returns the result
         gen_sql_update_hist (l_o_table_name
                            , l_hist_column_tab
                            , p_di_gui
                            , l_max_di_gui_hist
                            , TO_CHAR (l_cutoff_day, 'DD.MM.YYYY')
                            , l_sql_upd
                             );
         --  generate SQL Statement which inserts new time intervals
         --  for changed entities:
         --  l_sql_ins_versioned returns the result
         gen_sql_insert_hist_versioned (l_o_table_name
                                      , l_hist_column_tab
                                      , p_di_gui
                                      , l_max_di_gui_hist
                                      , TO_CHAR (l_cutoff_day, 'DD.MM.YYYY')
                                      , l_sql_ins_versioned
                                       );
      ELSE
         l_sql_upd_versioned    := NULL;
         l_sql_upd              := NULL;
         l_sql_ins_versioned    := NULL;
      END IF;

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG (l_sql_upd);
         log.LOG (l_sql_upd_versioned);
         log.LOG (l_sql_ins_versioned);
         log.LOG (l_sql_ins);
         p_log_message    := 'Genrated SQL Statement for History Table (do_not_execute = Y):';
         p_log_message    := p_log_message || l_sql_ins_versioned;
      ELSE
         IF l_hist_column_tab.COUNT > 1
         THEN
            DBMS_APPLICATION_INFO.set_action ('Start non versioned update in Table' || l_hist_table_name);
            log.LOG ('Start non versioned update in Table ' || l_hist_table_name);

            EXECUTE IMMEDIATE l_sql_upd;

            l_updated_rows               := SQL%ROWCOUNT;
            log.LOG ('Updated (unversioned) ' || l_updated_rows || ' rows in Table ' || l_hist_table_name);
            DBMS_APPLICATION_INFO.set_action ('Start versioned update in Table' || l_hist_table_name);
            log.LOG ('Start versioned update in Table ' || l_hist_table_name);

            EXECUTE IMMEDIATE l_sql_upd_versioned;

            l_updated_rows_versioned     := SQL%ROWCOUNT;
            log.LOG ('Updated (versioned) ' || l_updated_rows_versioned || ' rows in Table ' || l_hist_table_name);
            DBMS_APPLICATION_INFO.set_action ('Start versioned insert into Table' || l_hist_table_name);
            log.LOG ('Start versioned insert into  Table ' || l_hist_table_name);

            EXECUTE IMMEDIATE l_sql_ins_versioned;

            l_inserted_rows_versioned    := SQL%ROWCOUNT;
            log.LOG ('Inserted (versioned) ' || l_inserted_rows_versioned || ' rows in Table ' || l_hist_table_name);
            p_log_message                := 'CUTOFF_DAY is : ' || TO_CHAR (l_cutoff_day, 'DD.MM.YYYY') || CHR (10);
            p_log_message                := p_log_message || 'Updated  (unversioned) rows in   History Table ' || l_hist_table_name || ' : ' || l_updated_rows || CHR (10);
            p_log_message                :=
               p_log_message || 'Updated  (versioned)   rows in   History Table ' || l_hist_table_name || ' : (' || l_inserted_rows_versioned || ' ins + ' || l_updated_rows_versioned || ' upd)'
               || CHR (10);
         END IF;

         DBMS_APPLICATION_INFO.set_action ('Start insert into Table' || l_hist_table_name);
         log.LOG ('Start insert into Table ' || l_hist_table_name);

         EXECUTE IMMEDIATE l_sql_ins;

         l_inserted_rows    := SQL%ROWCOUNT;
         log.LOG ('Inserted ' || l_inserted_rows || ' rows into Table ' || l_hist_table_name);
         p_log_message      := p_log_message || 'Inserted (unversioned) rows into History Table ' || l_hist_table_name || ' : ' || l_inserted_rows;
         COMMIT;
         write_statistics (l_hist_table_name
                         , 'historization'
                         , p_di_gui
                         , l_inserted_rows
                         , l_updated_rows
                         , l_updated_rows_versioned
                         , 0
                         , 0
                          );
         log.LOG (p_log_message);
      END IF;

      log.LOG ('Fill History Table ' || l_hist_table_name, 'END');
      RETURN g_ret_ok;
   EXCEPTION
      WHEN OTHERS
      THEN
         log.set_console_logging (TRUE);
         log.LOG ('Fill History Table ' || l_hist_table_name, 'ERROR');
         log.LOG (SQLERRM
                        , log.gc_fatal
                        , SQLCODE
                         );
         RAISE;
   END fill_hist_tab;

   FUNCTION fill_r_tab (
      p_job_name              IN       VARCHAR2
    , p_workflow_name         IN       VARCHAR2
    , p_di_gui                IN       NUMBER
    , p_src_desc              IN       VARCHAR2                                                                                                              -- Table abbrevation e.g. CONTRACT,CUSTOMER
    , p_src_name              IN       VARCHAR2                                                                                                                          -- synonym e.g. EDWH_CORE.OCONT
    , p_sk_column_name_r      IN       VARCHAR2
    , p_sk_column_name_src    IN       VARCHAR2
    , p_nk_column_name_b1     IN       VARCHAR2
    , p_nk_column_name_src1   IN       VARCHAR2
    , p_nk_column_name_b2     IN       VARCHAR2                                                                                                                                    -- optional second NK
    , p_nk_column_name_src2   IN       VARCHAR2                                                                                                                                    -- optional second NK
    , p_id_column_name_b      IN       VARCHAR2
    , p_id_column_name_src    IN       VARCHAR2
    , p_log_message           OUT      VARCHAR2
    , p_nk_src_is_varchar     IN       VARCHAR2 DEFAULT 'N'
    , p_do_not_execute        IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER
   IS
      c_proc_name         CONSTANT VARCHAR2 (35)    := UPPER ('fill_r_tab ');
      l_max_di_gui_src             NUMBER;
      l_last_di_gui_src_imported   NUMBER;
      l_sql                        VARCHAR2 (32000);
      l_row_cnt                    NUMBER;
      l_row_cnt_ins                NUMBER;
      l_row_cnt_upd                NUMBER;
      l_table_name_r               VARCHAR2 (50);                                                                                                                       --> R Table     e.g. CONTRACT_R
      l_table_name_b               VARCHAR2 (50);                                                                                                                       --> B Table     e.g. CONTRACT_B
      l_table_name_cls             VARCHAR2 (50);                                                                                                             --> CLS Tabelle   e.g. CLS_RCONTRACT_CORE
      l_table_name_src             VARCHAR2 (1000);                                                                                                                 --> Source Table Synomym e.g. OCONT
      l_src_desc                   VARCHAR2 (50);                                                                                                                   --> Table abbrevation e.g. CONTRACT
      l_di_gui                     VARCHAR2 (50);
      l_date_first                 VARCHAR2 (50);
      l_date_last                  VARCHAR2 (50);
      l_date_dummy                 VARCHAR2 (50);
      l_seq_name_r                 VARCHAR2 (50);
      l_seq_name_b                 VARCHAR2 (50);
      l_ret_set_param              NUMBER;
      l_get_param                  VARCHAR2 (50);
      l_nk_column_name_b2          VARCHAR2 (30);
      l_nk_column_name_src2        VARCHAR2 (30);
      l_nkstr_values               VARCHAR2 (30);
      l_nkstr_insert               VARCHAR2 (30);
   BEGIN
      -- Set Local Variables
      l_date_first                  := 'TO_DATE(''11.11.1111'',''dd.mm.yyyy'')';
      l_date_last                   := 'TO_DATE(''09.09.9999'',''dd.mm.yyyy'')';
      l_date_dummy                  := 'TO_DATE(''08.08.8888'',''dd.mm.yyyy'')';
      l_table_name_r                := c_core_user || '.' || p_src_desc || '_R';
      l_table_name_b                := c_core_user || '.' || p_src_desc || '_B';
      l_table_name_cls              := c_cls_user || '.' || 'CLS_R' || p_src_desc || '_CORE';
      l_di_gui                      := TO_CHAR (p_di_gui);
      l_src_desc                    := TRIM (UPPER (p_src_desc));
      l_seq_name_r                  := c_core_user || '.S_' || l_src_desc || '_R_SK.NEXTVAL';
      l_seq_name_b                  := c_core_user || '.S_' || l_src_desc || '_B_ID.NEXTVAL';
      l_max_di_gui_src              := -1;
      l_last_di_gui_src_imported    := -1;
      l_row_cnt                     := 0;
      l_get_param                   := '';
      p_log_message                 := l_table_name_r || ': ';

      IF     p_nk_column_name_b2 IS NULL
         AND p_nk_column_name_src2 IS NULL
      THEN
         -- dummy NKs
         l_nk_column_name_b2      := p_nk_column_name_b1;
         l_nk_column_name_src2    := p_nk_column_name_src1;
         l_nkstr_values           := '';
         l_nkstr_insert           := '';
      ELSE
         -- real NKs
         l_nk_column_name_b2      := p_nk_column_name_b2;
         l_nk_column_name_src2    := p_nk_column_name_src2;
         l_nkstr_values           := ',new_rows.nk2';
         l_nkstr_insert           := ',' || p_nk_column_name_b2;
      END IF;

      IF p_nk_src_is_varchar = 'N'
      THEN
         l_table_name_src    :=
               '(SELECT DI_GUI,VALID_FROM,VALID_TO,('
            || p_nk_column_name_src1
            || ') as nk1,('
            || l_nk_column_name_src2
            || ') as nk2,'
            || p_sk_column_name_src
            || ' as src_sk,'
            || p_id_column_name_src
            || ' as src_id FROM '
            || c_core_user
            || '.'
            || p_src_name
            || ') ';
      ELSE
         l_table_name_src    :=
               '(SELECT DI_GUI,VALID_FROM,VALID_TO,TRIM('
            || p_nk_column_name_src1
            || ') as nk1,TRIM('
            || l_nk_column_name_src2
            || ') as nk2,'
            || p_sk_column_name_src
            || ' as src_sk,'
            || p_id_column_name_src
            || ' as src_id FROM '
            || c_core_user
            || '.'
            || p_src_name
            || ') ';
      END IF;

--> Get Parameter l_last_di_gui_src_imported for the incremental load
      l_get_param                   := pkg_utl_parameter.get_parameter (p_param_desc      => 'LAST_IMPORTED_SRC_GUI_ID'
                                                                      , p_system          => 'FRAMEWORK FILL_R_TAB'
                                                                      , p_sub_system      => l_table_name_r
                                                                       );

      IF l_get_param IS NULL
      THEN
         l_last_di_gui_src_imported    := -1;
      ELSE
         l_last_di_gui_src_imported    := TO_NUMBER (l_get_param);
      END IF;

      EXECUTE IMMEDIATE ('select max(di_gui) from ' || l_table_name_src || ' where di_gui > ' || l_last_di_gui_src_imported)
                   INTO l_max_di_gui_src;

      IF l_max_di_gui_src IS NULL
      THEN
         l_max_di_gui_src    := -1;
      END IF;

      -- 0: Job Start
      DBMS_APPLICATION_INFO.set_module (c_package_name, c_proc_name);
      DBMS_APPLICATION_INFO.set_client_info (p_job_name || ': ' || c_proc_name);
      DBMS_APPLICATION_INFO.set_action ('started');
      log.set_di_gui ((p_di_gui));
      log.set_job_name (p_job_name);
      log.LOG ('Fill R Table ' || l_table_name_r || ' (from ' || l_table_name_src || ' last imported_di_gui ' || TO_CHAR (l_last_di_gui_src_imported) || ')', '0-START');
-- 1: the new or changed Rows will be Inserted/Updated in the B Table
--    Propagation Subscriber ID/NK
      l_sql                         :=
            '
MERGE INTO '
         || l_table_name_b
         || ' old_rows USING
(
  SELECT  /*+ use_hash (src core)*/
  CASE
    WHEN core.'
         || p_src_desc
         || '_B_ID IS NULL
    THEN -9999
    ELSE core.'
         || p_src_desc
         || '_B_ID 
  END AS B_ID ,
  src.src_id,
  src.nk1,
  src.nk2
    FROM
      (
         SELECT nk1,nk2,MAX(src_id) AS src_id FROM '
         || l_table_name_src
         || '
         GROUP BY nk1,nk2
      ) src,
      '
         || l_table_name_b
         || ' core
    WHERE 
	      src.nk1 = core.'
         || p_nk_column_name_b1
         || ' (+)
    AND src.nk2 = core.'
         || l_nk_column_name_b2
         || ' (+)
    AND src.src_id <> -1
    AND
      -- new and changed rows
      ( 
        DECODE(src.nk1,core.'
         || p_nk_column_name_b1
         || ',0,1)        = 1
        OR DECODE(src.nk2 ,core.'
         || l_nk_column_name_b2
         || ',0,1)   = 1
        OR DECODE(src.src_id ,core.'
         || p_id_column_name_b
         || ',0,1) = 1 
      )
) new_rows 
ON (old_rows.'
         || p_src_desc
         || '_B_ID = new_rows.B_ID)
WHEN MATCHED THEN
  UPDATE
  SET 
    old_rows.'
         || p_id_column_name_b
         || ' = new_rows.src_id,
    old_rows.DI_GUI = '
         || l_di_gui
         || ' ,
    old_rows.DI_SYSTEM = ''SUBSCRIBER''
WHEN NOT MATCHED THEN
  INSERT ( '
         || p_src_desc
         || '_B_ID ,'
         || p_nk_column_name_b1
         || l_nkstr_insert
         || ','
         || p_id_column_name_b
         || '
      ,DI_GUI
      ,DI_GUI_INS
      ,DI_SYSTEM
    )
    VALUES
    (
       '
         || l_seq_name_b
         || ',new_rows.nk1'
         || l_nkstr_values
         || ',new_rows.src_id,'
         || l_di_gui
         || ','
         || l_di_gui
         || '
         ,''SUBSCRIBER''
    )
';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '1-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step1: Insert B Table ' || l_table_name_b);
         log.LOG ('Step1: Insert B Table ' || l_table_name_b, '1-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt    := SQL%ROWCOUNT;
         --> Set Table Statistics
         write_statistics (l_table_name_r
                         , 'b table (subscriber propag)'
                         , p_di_gui
                         , l_row_cnt
                         , 0
                         , 0
                         , 0
                         , 0
                          );
         COMMIT;
      END IF;

-- 2: all new records in the B Table, must be saved in the R Table
      l_sql                         :=
            '
INSERT  /*+ append */ INTO '
         || l_table_name_r
         || '
  ('
         || l_src_desc
         || '_R_SK ,'
         || p_sk_column_name_r
         || ','
         || l_src_desc
         || '_B_ID ,
    DI_GUI ,
    DI_GUI_INS ,
    VALID_FROM ,
    VALID_TO ,
    ACTIVE
  )
SELECT /*+ USE_HASH (SRC TGT) FULL(SRC) FULL(TGT)*/ '
         || l_seq_name_r
         || ',
         -1 ,
         SRC.'
         || l_src_desc
         || '_B_ID,'
         || l_di_gui
         || ','
         || l_di_gui
         || ','
         || l_date_first
         || ','
         || l_date_last
         || ',
         1
from '
         || l_table_name_b
         || ' SRC ,
  (SELECT '
         || l_src_desc
         || '_B_ID FROM '
         || l_table_name_r
         || ' ) TGT
WHERE 
SRC.'
         || l_src_desc
         || '_B_ID = TGT.'
         || l_src_desc
         || '_B_ID (+)
AND TGT.'
         || l_src_desc
         || '_B_ID  IS NULL';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '2-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step2: Insert R Table ' || l_table_name_r);
         log.LOG ('Step2: Insert R Table ' || l_table_name_r, '2-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt    := SQL%ROWCOUNT;
         --> Set Table Statistics
         write_statistics (l_table_name_r
                         , 'r table (b_table propag)'
                         , p_di_gui
                         , l_row_cnt
                         , 0
                         , 0
                         , 0
                         , 0
                          );
         COMMIT;
      END IF;

-- 3: Clean Cleansing Temporary Table
      l_sql                         := 'TRUNCATE TABLE ' || l_table_name_cls;

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '3-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action (l_sql);
         log.LOG (l_sql, '3-SQL');

         EXECUTE IMMEDIATE l_sql;
      END IF;

-- 4: Identification R Table's Periods to be delete  (obsolete Periods)
      l_sql                         :=
            '
INSERT  /*+ append*/ INTO '
         || l_table_name_cls
         || '
  (
    OLD_R_SK,
    TECH_OPERATION,
    TECH_INS_DT
  )
SELECT /*+ use_hash (DWH ORIG) */
   DWH.OLD_R_SK,
  ''D'',
  SYSDATE
FROM
  (select /*+ use_hash (R B) */
    R.'
         || l_src_desc
         || '_R_SK AS OLD_R_SK,
    R.VALID_FROM,
    B.'
         || p_nk_column_name_b1
         || ' AS NK1,
    B.'
         || l_nk_column_name_b2
         || ' AS NK2
  FROM  '
         || l_table_name_r
         || ' R,
        '
         || l_table_name_b
         || ' B
  WHERE R.'
         || l_src_desc
         || '_B_ID  = B.'
         || l_src_desc
         || '_B_ID
  AND R.VALID_FROM  <> '
         || l_date_first
         || '
  AND R.VALID_FROM  <> '
         || l_date_dummy
         || '
  ) DWH,
  (SELECT NK1,NK2,TRUNC(VALID_FROM) AS VALID_FROM
  FROM '
         || l_table_name_src
         || '
  WHERE TRUNC(VALID_TO) > TRUNC(VALID_FROM)
  ) ORIG
WHERE 
    DWH.NK1           = ORIG.NK1(+)
AND DWH.NK2           = ORIG.NK2(+)
AND DWH.VALID_FROM    = ORIG.VALID_FROM (+)
AND ORIG.VALID_FROM IS NULL';
      l_row_cnt                     := 0;

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '4-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step4: identify Periods to be delete: Table' || l_table_name_cls);
         log.LOG ('Step4: identify Periods to be delete: Table' || l_table_name_cls, '4-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt    := SQL%ROWCOUNT;
         COMMIT;
      END IF;

      IF l_row_cnt > 0
      THEN
         --> no delete possible!!
         p_log_message    := p_log_message || ' Identify Record to be Delete !!! Count = ' || TO_CHAR (l_row_cnt) || '-->> ERROR please kontakt the DATAMART Team!!';
         --> send email
         RETURN g_ret_nok;
      END IF;

-- 5: Identify all changes between Target and Source
      --    I =  Insert (new Periods),
      --    V =  Version (close Period)
      --    D =  Delete
      --    UC = Update change (at least a field has a changed value)
      --    US = Update set    (update default values with a right value)
      l_sql                         :=
            '
INSERT /*+ APPEND */ INTO '
         || l_table_name_cls
         || '
(
    OLD_R_SK,
    OLD_SK,
    OLD_VALID_TO,
    NEW_SK,
    NEW_VALID_TO,
    NEW_B_ID,
    NEW_VALID_FROM,
    TECH_OPERATION,
    TECH_INS_DT,
    TECH_LAST_DI_GUI
)    
SELECT /*+ USE_HASH (OLD_R_TABLE NEW_R_TABLE)*/
    NVL(OLD_R_TABLE.'
         || l_src_desc
         || '_R_SK,-99999) as OLD_R_SK,
    OLD_R_TABLE.'
         || p_sk_column_name_r
         || ' as OLD_SK,
    OLD_R_TABLE.VALID_TO as OLD_VALID_TO,
    NEW_R_TABLE.SRC_SK as NEW_SK,
    NEW_R_TABLE.VALID_TO as NEW_VALID_TO,
    NEW_R_TABLE.B_ID as NEW_B_ID,
    NEW_R_TABLE.VALID_FROM AS NEW_VALID_FROM,
    CASE 
      WHEN OLD_R_TABLE.'
         || l_src_desc
         || '_R_SK is null then ''I''    
      WHEN NEW_R_TABLE.VALID_TO <> OLD_R_TABLE.VALID_TO and OLD_R_TABLE.VALID_TO = '
         || l_date_last
         || ' then ''V''
      WHEN NEW_R_TABLE.VALID_TO <> OLD_R_TABLE.VALID_TO and OLD_R_TABLE.VALID_TO <> '
         || l_date_last
         || ' then ''UC''
      WHEN NEW_R_TABLE.src_sk <> OLD_R_TABLE.'
         || p_sk_column_name_r
         || ' and OLD_R_TABLE.'
         || p_sk_column_name_r
         || ' <> -1 then ''UC''      
    ELSE ''US''
    END as TECH_OPERATION,
    SYSDATE as TECH_INS_DT,
    NEW_R_TABLE.LAST_DI_GUI as TECH_LAST_DI_GUI
FROM 
(
SELECT
  /*+ USE_HASH (orig b)*/
  orig.src_sk,
  CASE
    WHEN orig.RANKING_FIRST_ROW = 1
    THEN '
         || l_date_first
         || '
    ELSE orig.VALID_FROM
  END  AS VALID_FROM,
  NVL(orig.VALID_TO,'
         || l_date_last
         || ') AS VALID_TO,
  b.'
         || l_src_desc
         || '_B_ID as B_ID,
  orig.LAST_DI_GUI
FROM
  (
    SELECT 
     nk1,
     nk2,
     src_sk,
     VALID_FROM,
     LEAD (VALID_FROM) over (partition BY nk1,nk2 order by VALID_FROM ASC) AS VALID_TO ,
     ROW_NUMBER () over (partition by nk1,nk2 order by VALID_FROM asc) AS RANKING_FIRST_ROW,
     LAST_DI_GUI       
    FROM 
	(
  	  SELECT 
	    NK1,
	    NK2,
	    SRC_SK,
	    TRUNC(VALID_FROM)     as  VALID_FROM,
	    ROW_NUMBER () over (partition BY nk1,nk2,TRUNC(VALID_FROM) order by SRC_SK ASC) AS RANKING_DOUBLE_ROW,
	    MAX (DI_GUI ) over (partition BY nk1,nk2) AS LAST_DI_GUI
	  FROM '
         || l_table_name_src
         || '
	  WHERE 
	    TRUNC(VALID_TO) > TRUNC(VALID_FROM) ---> nur gültige Periode 
	) orig_full
   WHERE 
    RANKING_DOUBLE_ROW = 1 -- ausschliessen mögliche Dubletten
    AND LAST_DI_GUI > '
         || l_last_di_gui_src_imported
         || '  --> delta load
  ) orig,
  '
         || l_table_name_b
         || ' b
WHERE 
    ORIG.nk1 = B.'
         || p_nk_column_name_b1
         || ' AND ORIG.nk2 = B.'
         || l_nk_column_name_b2
         || '
AND b.'
         || l_src_desc
         || '_B_ID <> -1
) NEW_R_TABLE
,'
         || l_table_name_r
         || ' OLD_R_TABLE
WHERE 
  NEW_R_TABLE.B_ID = OLD_R_TABLE.'
         || l_src_desc
         || '_B_ID(+)
  AND NEW_R_TABLE.VALID_FROM        = OLD_R_TABLE.VALID_FROM(+)
  AND 
  ( 
   DECODE(NEW_R_TABLE.VALID_TO,OLD_R_TABLE.VALID_TO,0,1) = 1 -- Close Period / Change Period / New Period
   OR 
   DECODE(NEW_R_TABLE.src_sk,OLD_R_TABLE.'
         || p_sk_column_name_r
         || ',0,1) = 1  -- Change SK
  )';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '5-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step5: Identify all changes between Target and Source :' || l_table_name_cls);
         log.LOG ('Step5: Identify all changes between Target and Source :' || l_table_name_cls, '5-SQL');

         EXECUTE IMMEDIATE l_sql;

         COMMIT;
      END IF;

-- Identify changed Records (at least a field has a changed value)
      l_row_cnt                     := 0;

      EXECUTE IMMEDIATE ('select count(*) from ' || l_table_name_cls || ' where TECH_OPERATION = ''UC''')
                   INTO l_row_cnt;

      IF l_row_cnt > 0
      THEN
         --> Datamart must be refreshed
         --> send email
         p_log_message    := p_log_message || ' DATAMART must be refreshed!! record changed: ' || TO_CHAR (l_row_cnt) || ', ';
         l_row_cnt        := 0;
      END IF;

-- 6: R Table Update (period change) - versioning and inplace updates

      -- in BODI we use ALTER SESSION FORCE PARALLEL DML PARALLEL 16 per default
      -- for the next 2 steps we need parallel = 1 (only one commit atfer two transactions).
      -- with parallel 16 will get ORA-12838: Objekt kann nach paralleler Änderung nicht gelesen/geändert werden
      EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DML PARALLEL 1';

      l_sql                         :=
            'MERGE INTO '
         || l_table_name_r
         || ' TGT USING
(SELECT 
  OLD_R_SK ,
  NEW_SK,
  NEW_VALID_TO,
  NEW_B_ID,
  NEW_VALID_FROM,
  CASE
    WHEN NEW_VALID_TO = '
         || l_date_last
         || '
    THEN 1
    ELSE 0
  END AS NEW_ACTIVE,
  '
         || l_di_gui
         || ' AS DI_GUI
FROM '
         || l_table_name_cls
         || '
WHERE TECH_OPERATION NOT IN (''I'',''D'')
) SRC ON (tgt.'
         || l_src_desc
         || '_R_SK = src.OLD_R_SK)
WHEN matched THEN
  UPDATE
  SET 
    ACTIVE        = SRC.NEW_ACTIVE ,
    VALID_TO      = SRC.NEW_VALID_TO,
    '
         || p_sk_column_name_r
         || ' = SRC.NEW_SK,
    DI_GUI        = SRC.DI_GUI';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '6-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step6: Update R Table ' || l_table_name_r);
         log.LOG ('Step6: Update R Table ' || l_table_name_r, '6-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt_upd    := SQL%ROWCOUNT;
      END IF;

-- 7: R Table Inserts - new periods
      l_sql                         :=
            'INSERT /*+ APPEND */ INTO '
         || l_table_name_r
         || '
( '
         || l_src_desc
         || '_R_SK ,'
         || p_sk_column_name_r
         || ','
         || l_src_desc
         || '_B_ID ,
    DI_GUI ,
    DI_GUI_INS ,
    VALID_FROM ,
    VALID_TO ,
    ACTIVE
    )
SELECT '
         || l_seq_name_r
         || ',
NEW_SK ,
NEW_B_ID,'
         || l_di_gui
         || ' ,'
         || l_di_gui
         || ' ,
NEW_VALID_FROM ,
NEW_VALID_TO,
CASE
    WHEN NEW_VALID_TO = '
         || l_date_last
         || '
    THEN 1
    ELSE 0
END AS NEW_ACTIVE
FROM '
         || l_table_name_cls
         || '
WHERE TECH_OPERATION = ''I''';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '7-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step7: Insert R Table ' || l_table_name_r);
         log.LOG ('Step7: Insert R Table ' || l_table_name_r, '7-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt_ins    := SQL%ROWCOUNT;
      END IF;

      --> only 1 commit after the update and insert transaction
      COMMIT;

      IF p_do_not_execute = 'N'
      THEN
         --> Set Parameter l_max_di_gui_src for the incremental load
         l_ret_set_param    :=
            pkg_utl_parameter.set_parameter (p_param_desc       => 'LAST_IMPORTED_SRC_GUI_ID'
                                           , p_param_value      => TO_CHAR (l_max_di_gui_src - 1)
                                           , p_system           => 'FRAMEWORK FILL_R_TAB'
                                           , p_sub_system       => l_table_name_r
                                            );
         --> Set Table Statistics
         write_statistics (l_table_name_r
                         , 'R table dataload'
                         , p_di_gui
                         , l_row_cnt_ins
                         , l_row_cnt_upd
                         , 0
                         , 0
                         , 0
                          );
      END IF;

      p_log_message                 := p_log_message || ' END Fill R Table -> Inserts: ' || TO_CHAR (l_row_cnt_ins) || ', Updates: ' || TO_CHAR (l_row_cnt_upd);

      EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DML PARALLEL 4';

-- 8: R Table Inserts - dummy Rows for inconsistent Subscriber SK's ( in the R table we need all Subscriber SK's)
      l_sql                         := 'DELETE FROM ' || l_table_name_r || ' WHERE VALID_FROM = ' || l_date_dummy;

      EXECUTE IMMEDIATE (l_sql);

      COMMIT;
      l_sql                         :=
            'INSERT /*+ APPEND */ INTO '
         || l_table_name_r
         || '
    (    '
         || l_src_desc
         || '_R_SK ,'
         || p_sk_column_name_r
         || ','
         || l_src_desc
         || '_B_ID ,
    DI_GUI ,
    DI_GUI_INS ,
    VALID_FROM ,
    VALID_TO ,
    ACTIVE
    )
SELECT '
         || l_seq_name_r
         || ',src_sk'
         || ',b.'
         || l_src_desc
         || '_B_ID,'
         || l_di_gui
         || ','
         || l_di_gui
         || ','
         || l_date_dummy
         || ','
         || l_date_dummy
         || ','
         || '0 as ACTIVE
FROM 
  ( SELECT /*+ no_merge*/ DISTINCT src_sk,
    nk1,
    nk2
    FROM
    '
         || l_table_name_src
         || '
  ) a,
  '
         || l_table_name_r
         || ' r,
  '
         || l_table_name_b
         || ' b
WHERE 
    a.src_sk               = r.'
         || p_sk_column_name_r
         || ' (+)
AND a.nk1                  = b.'
         || p_nk_column_name_b1
         || '
AND a.nk2                  = b.'
         || l_nk_column_name_b2
         || '
AND r.'
         || p_sk_column_name_r
         || ' IS NULL';

      IF p_do_not_execute = 'Y'
      THEN
         log.LOG ('(do_not_execute = Y): ' || l_sql, '8-SQL');
      ELSE
         DBMS_APPLICATION_INFO.set_action ('Step8: Insert R Table ' || l_table_name_r);
         log.LOG ('Step8: Insert R Table ' || l_table_name_r, '8-SQL');

         EXECUTE IMMEDIATE l_sql;

         l_row_cnt        := SQL%ROWCOUNT;
         p_log_message    := p_log_message || ' Inserted Dummy Rows: ' || TO_CHAR (l_row_cnt);
         COMMIT;
         --> Set Table Statistics
         write_statistics (l_table_name_r
                         , 'R table (SK dummy periods)'
                         , p_di_gui
                         , l_row_cnt
                         , 0
                         , 0
                         , 0
                         , 0
                          );
      END IF;

-- 9: Job End
      log.LOG (p_log_message || ' last_gui_imported: ' || TO_CHAR (l_max_di_gui_src), '9-END');
      RETURN g_ret_ok;
   EXCEPTION
      WHEN OTHERS
      THEN
         log.set_console_logging (TRUE);
         log.LOG ('Fill R Table ' || l_table_name_r, 'ERROR');
         log.LOG (SQLERRM
                        , log.gc_fatal
                        , SQLCODE
                         );
         ROLLBACK;
         RAISE;
   END fill_r_tab;
/**
 * Package initialization
 */
BEGIN
   -- Set package-constants
   c_body_version    := '$Id: pkg_etl_core-impl.sql 1631 2011-10-20 11:10:16Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_core/pkg_etl_core-impl.sql $';
   c_package_name    := $$plsql_unit;
   -- Differentiate for execution in test or production
   c_devel_mode      := (SUBSTR (UPPER (SYS_CONTEXT ('USERENV', 'DB_NAME'))
                               , 1
                               , 5
                                ) = 'EDWHD');

   IF c_devel_mode
   THEN
      log.set_console_logging (FALSE);
      log.set_table_logging (TRUE);
      log.set_log_level (log.gc_all);
      log.LOG ('Initialized in development mode.');
   ELSE
      log.set_console_logging (FALSE);
      log.set_table_logging (TRUE);
      log.LOG ('Initialized in production mode.');
   END IF;
END pkg_etl_core;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('pkg_etl_framework'
                                 , 'pkg_etl_framework'
                                 , TRUE
                                  );
END;
/

SHOW errors