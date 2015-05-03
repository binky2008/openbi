CREATE OR REPLACE PACKAGE BODY p#frm#stag_meta
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_meta-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_meta/stag_meta-impl.sql $
   */
   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   /**
   * Table containing dictionary values
   */
   TYPE r_column IS RECORD (
      stag_column_pos         NUMBER
    , stag_column_name        VARCHAR2 (4000)
    , stag_column_comment     VARCHAR2 (4000)
    , stag_column_type        VARCHAR2 (4000)
    , stag_column_length      VARCHAR2 (4000)
    , stag_column_precision   VARCHAR2 (4000)
    , stag_column_scale       VARCHAR2 (4000)
    , stag_column_def         VARCHAR2 (4000)
    , stag_column_nk_pos      NUMBER
   );

   TYPE t_t_columns IS TABLE OF r_column;

   l_t_columns      t_t_columns := NULL;
   /**
   * Other types
   */
   l_sql_col_def    CLOB := p#frm#dict.c_sql_col_def;
   l_n_pk_pos_max   NUMBER;

   FUNCTION fct_get_column_list (
      p_vc_object_id     IN NUMBER
    , p_vc_column_type   IN VARCHAR2
    , p_vc_list_type     IN VARCHAR2
    , p_vc_alias1        IN VARCHAR2 DEFAULT NULL
    , p_vc_alias2        IN VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      l_vc_list   t_string;
   BEGIN
      -- Build list of columns
      FOR r_col IN (  SELECT stag_column_name
                        FROM p#frm#stag_column_t
                       WHERE stag_object_id = p_vc_object_id
                         AND stag_column_edwh_flag = 1
                         AND (p_vc_column_type = 'ALL'
                           OR (p_vc_column_type = 'PK'
                           AND stag_column_nk_pos IS NOT NULL)
                           OR (p_vc_column_type = 'NPK'
                           AND stag_column_nk_pos IS NULL))
                    ORDER BY stag_column_nk_pos
                           , stag_column_pos) LOOP
         l_vc_list :=
               l_vc_list
            || CHR (10)
            || CASE p_vc_list_type
                  WHEN 'LIST_SIMPLE' THEN
                        r_col.stag_column_name
                     || ', '
                  WHEN 'LIST_ALIAS' THEN
                        p_vc_alias1
                     || '.'
                     || r_col.stag_column_name
                     || ', '
                  WHEN 'SET_ALIAS' THEN
                        p_vc_alias1
                     || '.'
                     || r_col.stag_column_name
                     || ' = '
                     || p_vc_alias2
                     || '.'
                     || r_col.stag_column_name
                     || ', '
                  WHEN 'LIST_NVL2' THEN
                        'NVL2 ('
                     || p_vc_alias1
                     || '.rowid, '
                     || p_vc_alias1
                     || '.'
                     || r_col.stag_column_name
                     || ', '
                     || p_vc_alias2
                     || '.'
                     || r_col.stag_column_name
                     || ') AS '
                     || r_col.stag_column_name
                     || ', '
                  WHEN 'AND_NOTNULL' THEN
                        r_col.stag_column_name
                     || ' IS NOT NULL AND '
                  WHEN 'AND_ALIAS' THEN
                        p_vc_alias1
                     || '.'
                     || r_col.stag_column_name
                     || ' = '
                     || p_vc_alias2
                     || '.'
                     || r_col.stag_column_name
                     || ' AND '
                  WHEN 'OR_DECODE' THEN
                        'DECODE ('
                     || p_vc_alias1
                     || '.'
                     || r_col.stag_column_name
                     || ', '
                     || p_vc_alias2
                     || '.'
                     || r_col.stag_column_name
                     || ', 0, 1) = 1 OR '
               END;
      END LOOP;

      IF p_vc_list_type IN ('LIST_SIMPLE', 'LIST_ALIAS', 'LIST_NVL2', 'SET_ALIAS') THEN
         l_vc_list :=
            RTRIM (
               l_vc_list
             , ', '
            );
      ELSIF p_vc_list_type IN ('AND_NOTNULL', 'AND_ALIAS') THEN
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

      RETURN l_vc_list;
   END fct_get_column_list;

   PROCEDURE prc_stat_type_ins (
      p_vc_type_code   IN VARCHAR2
    , p_vc_type_name   IN VARCHAR2
    , p_vc_type_desc   IN VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO p#frm#stag_stat_type_t trg
           USING (SELECT p_vc_type_code AS type_code
                       , p_vc_type_name AS type_name
                       , p_vc_type_desc AS type_desc
                    FROM DUAL) src
              ON (trg.stag_stat_type_code = src.type_code)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_stat_type_name = src.type_name
                  , trg.stag_stat_type_desc = src.type_desc
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_stat_type_code
                     , trg.stag_stat_type_name
                     , trg.stag_stat_type_desc
                    )
             VALUES (
                       src.type_code
                     , src.type_name
                     , src.type_desc
                    );

      COMMIT;
   END prc_stat_type_ins;

   PROCEDURE prc_source_ins (
      p_vc_source_code      IN VARCHAR2
    , p_vc_source_prefix    IN VARCHAR2
    , p_vc_source_name      IN VARCHAR2
    , p_vc_stage_owner      IN VARCHAR2
    , p_vc_ts_stg1_data     IN VARCHAR2
    , p_vc_ts_stg1_indx     IN VARCHAR2
    , p_vc_ts_stg2_data     IN VARCHAR2
    , p_vc_ts_stg2_indx     IN VARCHAR2
    , p_vc_fb_archive       IN VARCHAR2 DEFAULT NULL
    , p_vc_bodi_ds          IN VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds   IN VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      MERGE INTO p#frm#stag_source_t trg
           USING (SELECT p_vc_source_code AS source_code
                       , p_vc_source_prefix AS source_prefix
                       , p_vc_source_name AS source_name
                       , p_vc_stage_owner AS stage_owner
                       , p_vc_ts_stg1_data AS ts_stg1_data
                       , p_vc_ts_stg1_indx AS ts_stg1_indx
                       , p_vc_ts_stg2_data AS ts_stg2_data
                       , p_vc_ts_stg2_indx AS ts_stg2_indx
                       , p_vc_fb_archive AS fb_archive
                       , p_vc_bodi_ds AS bodi_ds
                       , p_vc_source_bodi_ds AS source_bodi_ds
                    FROM DUAL) src
              ON (trg.stag_source_code = src.source_code)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_source_prefix = src.source_prefix
                  , trg.stag_source_name = src.source_name
                  , trg.stag_owner = src.stage_owner
                  , trg.stag_ts_stage_data = src.ts_stg1_data
                  , trg.stag_ts_stage_indx = src.ts_stg1_indx
                  , trg.stag_ts_hist_data = src.ts_stg2_data
                  , trg.stag_ts_hist_indx = src.ts_stg2_indx
                  , trg.stag_fb_archive = src.fb_archive
                  , trg.stag_bodi_ds = src.bodi_ds
                  , trg.stag_source_bodi_ds = src.source_bodi_ds
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_source_code
                     , trg.stag_source_prefix
                     , trg.stag_source_name
                     , trg.stag_owner
                     , trg.stag_ts_stage_data
                     , trg.stag_ts_stage_indx
                     , trg.stag_ts_hist_data
                     , trg.stag_ts_hist_indx
                     , trg.stag_fb_archive
                     , trg.stag_bodi_ds
                     , trg.stag_source_bodi_ds
                    )
             VALUES (
                       src.source_code
                     , src.source_prefix
                     , src.source_name
                     , src.stage_owner
                     , src.ts_stg1_data
                     , src.ts_stg1_indx
                     , src.ts_stg2_data
                     , src.ts_stg2_indx
                     , src.source_bodi_ds
                     , src.bodi_ds
                     , src.source_bodi_ds
                    );

      COMMIT;
   END prc_source_ins;

   PROCEDURE prc_source_del (
      p_vc_source_code   IN VARCHAR2
    , p_b_cascade        IN BOOLEAN DEFAULT FALSE
   )
   IS
      l_n_source_id   NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_cnt
        FROM p#frm#stag_source_t
       WHERE stag_source_code = p_vc_source_code;

      IF l_n_cnt > 0 THEN
         -- Get the key object id
         SELECT stag_source_id
           INTO l_n_source_id
           FROM p#frm#stag_source_t
          WHERE stag_source_code = p_vc_source_code;

         IF NOT p_b_cascade THEN
            SELECT COUNT (*)
              INTO l_n_cnt
              FROM p#frm#stag_object_t
             WHERE stag_source_id = l_n_source_id;

            IF l_n_cnt > 0 THEN
               raise_application_error (
                  -20001
                , 'Cannot delete source with objects'
               );
            END IF;
         END IF;

         -- Delete children objects
         FOR r_obj IN (SELECT stag_object_name
                         FROM p#frm#stag_object_t
                        WHERE stag_source_id = l_n_source_id) LOOP
            prc_object_del (
               p_vc_source_code
             , r_obj.stag_object_name
             , p_b_cascade
            );
         END LOOP;

         DELETE p#frm#stag_source_db_t
          WHERE stag_source_id = l_n_source_id;

         DELETE p#frm#stag_source_t
          WHERE stag_source_code = p_vc_source_code;

         COMMIT;
      END IF;
   END prc_source_del;

   PROCEDURE prc_source_db_ins (
      p_vc_source_code          IN VARCHAR2
    , p_vc_distribution_code    IN VARCHAR2
    , p_vc_source_db_link       IN VARCHAR2
    , p_vc_source_owner         IN VARCHAR2
    , p_vc_source_db_jdbcname   IN VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds       IN VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      MERGE INTO p#frm#stag_source_db_t trg
           USING (SELECT stag_source_id
                       , p_vc_distribution_code AS distribution_code
                       , p_vc_source_db_link AS source_db_link
                       , p_vc_source_db_jdbcname AS source_db_jdbcname
                       , p_vc_source_owner AS source_owner
                       , p_vc_source_bodi_ds AS source_bodi_ds
                    FROM p#frm#stag_source_t
                   WHERE stag_source_code = p_vc_source_code) src
              ON (trg.stag_source_id = src.stag_source_id
              AND trg.stag_distribution_code = src.distribution_code)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_source_db_link = src.source_db_link
                  , trg.stag_source_db_jdbcname = src.source_db_jdbcname
                  , trg.stag_source_owner = src.source_owner
                  , trg.stag_source_bodi_ds = src.source_bodi_ds
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_source_id
                     , trg.stag_distribution_code
                     , trg.stag_source_db_link
                     , trg.stag_source_db_jdbcname
                     , trg.stag_source_owner
                     , trg.stag_source_bodi_ds
                    )
             VALUES (
                       src.stag_source_id
                     , src.distribution_code
                     , src.source_db_link
                     , src.source_db_jdbcname
                     , src.source_owner
                     , src.source_bodi_ds
                    );

      COMMIT;
   END prc_source_db_ins;

   PROCEDURE prc_object_ins (
      p_vc_source_code        IN VARCHAR2
    , p_vc_object_name        IN VARCHAR2
    , p_n_parallel_degree     IN NUMBER DEFAULT NULL
    , p_vc_filter_clause      IN VARCHAR2 DEFAULT NULL
    , p_vc_partition_clause   IN VARCHAR2 DEFAULT NULL
    , p_vc_hist_flag          IN NUMBER DEFAULT 1
    , p_vc_fbda_flag          IN NUMBER DEFAULT 0
    , p_vc_increment_buffer   IN NUMBER DEFAULT NULL
   )
   IS
      l_vc_table_comment   t_string;
   BEGIN
      -- Set object
      MERGE INTO p#frm#stag_object_t trg
           USING (SELECT stag_source_id
                       , p_vc_object_name AS object_name
                       , p_n_parallel_degree AS parallel_degree
                       , p_vc_filter_clause AS filter_clause
                       , p_vc_partition_clause AS partition_clause
                       , p_vc_hist_flag AS hist_flag
                       , p_vc_fbda_flag AS fbda_flag
                       , p_vc_increment_buffer AS increment_buffer
                    FROM p#frm#stag_source_t
                   WHERE stag_source_code = p_vc_source_code) src
              ON (trg.stag_source_id = src.stag_source_id
              AND trg.stag_object_name = src.object_name)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_parallel_degree = parallel_degree
                  , trg.stag_filter_clause = filter_clause
                  , trg.stag_partition_clause = partition_clause
                  , trg.stag_hist_flag = src.hist_flag
                  , trg.stag_fbda_flag = src.fbda_flag
                  , trg.stag_increment_buffer = src.increment_buffer
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_source_id
                     , trg.stag_object_name
                     , trg.stag_parallel_degree
                     , trg.stag_filter_clause
                     , trg.stag_partition_clause
                     , trg.stag_hist_flag
                     , trg.stag_fbda_flag
                     , trg.stag_increment_buffer
                    )
             VALUES (
                       src.stag_source_id
                     , src.object_name
                     , src.parallel_degree
                     , src.filter_clause
                     , src.partition_clause
                     , src.hist_flag
                     , src.fbda_flag
                     , src.increment_buffer
                    );

      COMMIT;

      -- Get object comment from source
      FOR r_obj IN (  SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_object_id
                           , stag_object_name
                        FROM (SELECT d.stag_source_db_link
                                   , d.stag_source_owner
                                   , o.stag_object_id
                                   , o.stag_object_name
                                   , ROW_NUMBER () OVER (PARTITION BY o.stag_object_id ORDER BY d.stag_source_db_id) AS source_db_order
                                FROM p#frm#stag_source_t s
                                   , p#frm#stag_source_db_t d
                                   , p#frm#stag_object_t o
                               WHERE s.stag_source_id = d.stag_source_id
                                 AND s.stag_source_id = o.stag_source_id
                                 AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                                 AND p_vc_object_name IN (o.stag_object_name, 'ALL'))
                       WHERE source_db_order = 1
                    ORDER BY stag_object_id) LOOP
         l_vc_table_comment :=
            p#frm#dict.fct_get_table_comment (
               r_obj.stag_source_db_link
             , r_obj.stag_source_owner
             , r_obj.stag_object_name
            );

         UPDATE p#frm#stag_object_t
            SET stag_object_comment = l_vc_table_comment
          WHERE stag_object_id = r_obj.stag_object_id;
      END LOOP;

      COMMIT;
   END prc_object_ins;

   PROCEDURE prc_object_del (
      p_vc_source_code   IN VARCHAR2
    , p_vc_object_name   IN VARCHAR2
    , p_b_cascade        IN BOOLEAN DEFAULT FALSE
   )
   IS
      l_n_object_id   NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_cnt
        FROM p#frm#stag_source_t s
           , p#frm#stag_object_t o
       WHERE s.stag_source_id = o.stag_source_id
         AND s.stag_source_code = p_vc_source_code
         AND o.stag_object_name = p_vc_object_name;

      IF l_n_cnt > 0 THEN
         -- Get the key object id
         SELECT o.stag_object_id
           INTO l_n_object_id
           FROM p#frm#stag_source_t s
              , p#frm#stag_object_t o
          WHERE s.stag_source_id = o.stag_source_id
            AND s.stag_source_code = p_vc_source_code
            AND o.stag_object_name = p_vc_object_name;

         IF NOT p_b_cascade THEN
            SELECT COUNT (*)
              INTO l_n_cnt
              FROM p#frm#stag_column_t
             WHERE stag_object_id = l_n_object_id;

            IF l_n_cnt > 0 THEN
               raise_application_error (
                  -20001
                , 'Cannot delete object with columns'
               );
            END IF;
         END IF;

         DELETE p#frm#stag_column_t
          WHERE stag_object_id = l_n_object_id;

         DELETE p#frm#stag_object_t
          WHERE stag_object_id = l_n_object_id;

         COMMIT;
      END IF;
   END prc_object_del;

   PROCEDURE prc_column_ins (
      p_vc_source_code       IN VARCHAR2
    , p_vc_object_name       IN VARCHAR2
    , p_vc_column_name       IN VARCHAR2
    , p_vc_column_name_map   IN VARCHAR2 DEFAULT NULL
    , p_vc_column_def        IN VARCHAR2 DEFAULT NULL
    , p_n_column_pos         IN NUMBER DEFAULT NULL
    , p_n_column_nk_pos      IN NUMBER DEFAULT NULL
    , p_n_column_incr_flag   IN NUMBER DEFAULT 0
    , p_n_column_hist_flag   IN NUMBER DEFAULT 1
    , p_n_column_edwh_flag   IN NUMBER DEFAULT 1
   )
   IS
   BEGIN
      MERGE INTO p#frm#stag_column_t trg
           USING (SELECT o.stag_object_id
                       , p_vc_object_name AS object_name
                       , p_vc_column_name AS column_name
                       , p_vc_column_name_map AS column_name_map
                       , p_vc_column_def AS column_def
                       , p_n_column_pos AS column_pos
                       , p_n_column_nk_pos AS column_nk_pos
                       , p_n_column_incr_flag AS column_incr_flag
                       , p_n_column_hist_flag AS column_hist_flag
                       , p_n_column_edwh_flag AS column_edwh_flag
                    FROM p#frm#stag_source_t s
                       , p#frm#stag_object_t o
                   WHERE s.stag_source_id = o.stag_source_id
                     AND s.stag_source_code = p_vc_source_code
                     AND o.stag_object_name = p_vc_object_name) src
              ON (trg.stag_object_id = src.stag_object_id
              AND trg.stag_column_name = src.column_name)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_column_name_map = NVL (src.column_name_map, trg.stag_column_name_map)
                  , trg.stag_column_def = NVL (src.column_def, trg.stag_column_def)
                  , trg.stag_column_pos = NVL (src.column_pos, trg.stag_column_pos)
                  , trg.stag_column_nk_pos = NVL (src.column_nk_pos, trg.stag_column_nk_pos)
                  , trg.stag_column_incr_flag = NVL (src.column_incr_flag, trg.stag_column_incr_flag)
                  , trg.stag_column_hist_flag = NVL (src.column_hist_flag, trg.stag_column_hist_flag)
                  , trg.stag_column_edwh_flag = NVL (src.column_edwh_flag, trg.stag_column_edwh_flag)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_object_id
                     , trg.stag_column_name
                     , trg.stag_column_name_map
                     , trg.stag_column_def
                     , trg.stag_column_pos
                     , trg.stag_column_nk_pos
                     , trg.stag_column_incr_flag
                     , trg.stag_column_hist_flag
                     , trg.stag_column_edwh_flag
                    )
             VALUES (
                       src.stag_object_id
                     , src.column_name
                     , src.column_name_map
                     , src.column_def
                     , src.column_pos
                     , src.column_nk_pos
                     , src.column_incr_flag
                     , src.column_hist_flag
                     , src.column_edwh_flag
                    );

      COMMIT;
   END prc_column_ins;

   PROCEDURE prc_column_del (
      p_vc_source_code   IN VARCHAR2
    , p_vc_object_name   IN VARCHAR2
    , p_vc_column_name   IN VARCHAR2
   )
   IS
   BEGIN
      DELETE p#frm#stag_column_t
       WHERE stag_object_id = (SELECT o.stag_object_id
                                 FROM p#frm#stag_source_t s
                                    , p#frm#stag_object_t o
                                WHERE s.stag_source_id = o.stag_source_id
                                  AND s.stag_source_code = p_vc_source_code
                                  AND o.stag_object_name = p_vc_object_name)
         AND stag_column_name = p_vc_column_name;

      COMMIT;
   END prc_column_del;

   PROCEDURE prc_column_import_from_source (
      p_vc_source_code         IN VARCHAR2
    , p_vc_object_name         IN VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN BOOLEAN DEFAULT TRUE
   )
   IS
      l_vc_prc_name   t_string := 'prc_column_import_from_source';
   BEGIN
      l_sql_col_def := p#frm#dict.c_sql_col_def;
      l_t_columns := NULL;
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Prepare metadata'
       , 'Start'
      );

      FOR r_obj IN (SELECT stag_object_id
                         , stag_object_name
                         , stag_hist_flag
                         , stag_owner
                         , stag_source_owner
                         , stag_source_db_link
                      FROM (SELECT o.stag_object_id
                                 , o.stag_object_name
                                 , o.stag_hist_flag
                                 , s.stag_owner
                                 , d.stag_source_owner
                                 , d.stag_source_db_link
                                 , ROW_NUMBER () OVER (PARTITION BY o.stag_object_id ORDER BY d.stag_source_db_id) AS db_rank
                              FROM p#frm#stag_object_t o
                                 , p#frm#stag_source_t s
                                 , p#frm#stag_source_db_t d
                             WHERE o.stag_source_id = s.stag_source_id
                               AND s.stag_source_id = d.stag_source_id
                               AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                               AND p_vc_object_name IN (o.stag_object_name, 'ALL'))
                     WHERE db_rank = 1) LOOP
         l_n_pk_pos_max := NULL;
         p#frm#dict.g_vc_src_obj_dblink := r_obj.stag_source_db_link;
         p#frm#dict.prc_set_text_param (
            l_sql_col_def
          , 'sql_obj_pk'
          , CASE
               WHEN p_b_check_dependencies THEN
                  p#frm#dict.c_sql_obj_col_pk
               ELSE
                  p#frm#dict.c_sql_obj_col_pk_nodep
            END
         );
         p#frm#dict.prc_set_src_param (l_sql_col_def);

         EXECUTE IMMEDIATE l_sql_col_def
            BULK COLLECT INTO l_t_columns
            USING r_obj.stag_source_owner
                , r_obj.stag_object_name;

         FOR i IN l_t_columns.FIRST .. l_t_columns.LAST LOOP
            MERGE INTO p#frm#stag_column_t trg
                 USING (SELECT l_t_columns (i).stag_column_name AS stag_column_name
                             , l_t_columns (i).stag_column_comment AS stag_column_comment
                             , l_t_columns (i).stag_column_pos AS stag_column_pos
                             , l_t_columns (i).stag_column_type AS stag_column_type
                             , l_t_columns (i).stag_column_length AS stag_column_length
                             , l_t_columns (i).stag_column_precision AS stag_column_precision
                             , l_t_columns (i).stag_column_scale AS stag_column_scale
                             , l_t_columns (i).stag_column_def AS stag_column_def
                             , l_t_columns (i).stag_column_nk_pos AS stag_column_nk_pos
                          FROM DUAL) src
                    ON (trg.stag_column_name = src.stag_column_name
                    AND trg.stag_object_id = r_obj.stag_object_id)
            WHEN MATCHED THEN
               UPDATE SET trg.stag_column_pos = src.stag_column_pos
                        , trg.stag_column_type = src.stag_column_type
                        , trg.stag_column_length = src.stag_column_length
                        , trg.stag_column_precision = src.stag_column_precision
                        , trg.stag_column_scale = src.stag_column_scale
                        , trg.stag_column_def = src.stag_column_def
                        , trg.stag_column_comment = src.stag_column_comment
                        , trg.stag_column_nk_pos = src.stag_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT     (
                             trg.stag_object_id
                           , trg.stag_column_pos
                           , trg.stag_column_name
                           , trg.stag_column_comment
                           , trg.stag_column_type
                           , trg.stag_column_length
                           , trg.stag_column_precision
                           , trg.stag_column_scale
                           , trg.stag_column_def
                           , trg.stag_column_nk_pos
                           , trg.stag_column_edwh_flag
                           , trg.stag_column_hist_flag
                          )
                   VALUES (
                             r_obj.stag_object_id
                           , src.stag_column_pos
                           , src.stag_column_name
                           , src.stag_column_comment
                           , src.stag_column_type
                           , src.stag_column_length
                           , src.stag_column_precision
                           , src.stag_column_scale
                           , src.stag_column_def
                           , src.stag_column_nk_pos
                           , 1
                           , 1
                          );

            l_n_pk_pos_max :=
               GREATEST (
                  NVL (l_t_columns (i).stag_column_nk_pos, 0)
                , NVL (l_n_pk_pos_max, 0)
               );
         END LOOP;

         UPDATE p#frm#stag_object_t
            SET stag_source_nk_flag =
                   CASE
                      WHEN l_n_pk_pos_max > 0 THEN
                         1
                      ELSE
                         0
                   END
          WHERE stag_object_id = r_obj.stag_object_id;

         COMMIT;
      END LOOP;

      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Prepare metadata'
       , 'Finish'
      );
   END prc_column_import_from_source;

   PROCEDURE prc_column_import_from_stage (
      p_vc_source_code         IN VARCHAR2
    , p_vc_object_name         IN VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN BOOLEAN DEFAULT TRUE
   )
   IS
      l_vc_prc_name   t_string := 'prc_column_import_from_stage';
   BEGIN
      l_sql_col_def := p#frm#dict.c_sql_col_def;
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Prepare metadata'
       , 'Start'
      );
      prc_set_object_properties;

      FOR r_obj IN (SELECT stag_owner
                         , stag_object_id
                         , stag_object_name
                         , stag_stage_table_name
                         , stag_hist_flag
                      FROM p#frm#stag_object_t o
                         , p#frm#stag_source_t s
                     WHERE o.stag_source_id = s.stag_source_id
                       AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                       AND p_vc_object_name IN (o.stag_object_name, 'ALL')) LOOP
         p#frm#dict.prc_set_text_param (
            l_sql_col_def
          , 'sql_obj_pk'
          , CASE
               WHEN p_b_check_dependencies THEN
                  p#frm#dict.c_sql_obj_col_pk
               ELSE
                  p#frm#dict.c_sql_obj_col_pk_nodep
            END
         );
         p#frm#dict.prc_set_src_param (l_sql_col_def);

         EXECUTE IMMEDIATE l_sql_col_def
            BULK COLLECT INTO l_t_columns
            USING r_obj.stag_owner
                , r_obj.stag_object_name;

         FOR i IN l_t_columns.FIRST .. l_t_columns.LAST LOOP
            MERGE INTO p#frm#stag_column_t trg
                 USING (SELECT l_t_columns (i).stag_column_name AS stag_column_name
                             , l_t_columns (i).stag_column_comment AS stag_column_comment
                             , l_t_columns (i).stag_column_pos AS stag_column_pos
                             , l_t_columns (i).stag_column_def AS stag_column_def
                             , l_t_columns (i).stag_column_nk_pos AS stag_column_nk_pos
                          FROM DUAL) src
                    ON (trg.stag_column_name = src.stag_column_name
                    AND trg.stag_object_id = r_obj.stag_object_id)
            WHEN MATCHED THEN
               UPDATE SET trg.stag_column_pos = src.stag_column_pos
                        , trg.stag_column_def = src.stag_column_def
                        , trg.stag_column_comment = src.stag_column_comment
                        , trg.stag_column_nk_pos = src.stag_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT     (
                             trg.stag_object_id
                           , trg.stag_column_pos
                           , trg.stag_column_name
                           , trg.stag_column_comment
                           , trg.stag_column_def
                           , trg.stag_column_nk_pos
                           , trg.stag_column_edwh_flag
                           , trg.stag_column_hist_flag
                          )
                   VALUES (
                             r_obj.stag_object_id
                           , src.stag_column_pos
                           , src.stag_column_name
                           , src.stag_column_comment
                           , src.stag_column_def
                           , src.stag_column_nk_pos
                           , 1
                           , 1
                          );

            l_n_pk_pos_max :=
               GREATEST (
                  NVL (l_t_columns (i).stag_column_nk_pos, 0)
                , l_n_pk_pos_max
               );
         END LOOP;

         UPDATE p#frm#stag_object_t
            SET stag_source_nk_flag =
                   CASE
                      WHEN l_n_pk_pos_max = 0 THEN
                         0
                      ELSE
                         1
                   END
          WHERE stag_object_id = r_obj.stag_object_id;

         COMMIT;
      END LOOP;

      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Prepare metadata'
       , 'Finish'
      );
   END prc_column_import_from_stage;

   PROCEDURE prc_check_column_changes (
      p_vc_source_code         IN VARCHAR2
    , p_vc_object_name         IN VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN BOOLEAN DEFAULT TRUE
   )
   IS
      l_vc_prc_name   t_string := 'prc_check_column_changes';
   BEGIN
      l_sql_col_def := p#frm#dict.c_sql_col_def;
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Check column changes'
       , 'Start'
      );

      FOR r_obj IN (  SELECT stag_source_id
                           , stag_source_code
                           , stag_source_db_link
                           , stag_source_owner
                           , stag_owner
                           , stag_object_id
                           , stag_object_name
                           , stag_stage_table_name
                        FROM (SELECT s.stag_source_id
                                   , s.stag_source_code
                                   , d.stag_source_db_link
                                   , d.stag_source_owner
                                   , s.stag_owner
                                   , o.stag_object_id
                                   , o.stag_object_name
                                   , o.stag_stage_table_name
                                   , ROW_NUMBER () OVER (PARTITION BY o.stag_object_id ORDER BY d.stag_source_db_id) AS source_db_order
                                FROM p#frm#stag_source_t s
                                   , p#frm#stag_source_db_t d
                                   , p#frm#stag_object_t o
                               WHERE s.stag_source_id = d.stag_source_id
                                 AND s.stag_source_id = o.stag_source_id
                                 AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                                 AND p_vc_object_name IN (o.stag_object_name, 'ALL'))
                       WHERE source_db_order = 1
                    ORDER BY stag_object_id) LOOP
         p#frm#dict.g_vc_src_obj_dblink := r_obj.stag_source_db_link;
         p#frm#dict.prc_set_text_param (
            l_sql_col_def
          , 'sql_obj_pk'
          , CASE
               WHEN p_b_check_dependencies THEN
                  p#frm#dict.c_sql_obj_col_pk
               ELSE
                  p#frm#dict.c_sql_obj_col_pk_nodep
            END
         );
         p#frm#dict.prc_set_src_param (l_sql_col_def);

         EXECUTE IMMEDIATE l_sql_col_def
            BULK COLLECT INTO l_t_columns
            USING r_obj.stag_source_owner
                , r_obj.stag_object_name;

         DELETE p#frm#stag_column_check_t
          WHERE stag_object_id = r_obj.stag_object_id;

         FOR i IN l_t_columns.FIRST .. l_t_columns.LAST LOOP
            MERGE INTO p#frm#stag_column_check_t trg
                 USING (SELECT l_t_columns (i).stag_column_name AS stag_column_name
                             , l_t_columns (i).stag_column_comment AS stag_column_comment
                             , l_t_columns (i).stag_column_pos AS stag_column_pos
                             , l_t_columns (i).stag_column_def AS stag_column_def
                             , l_t_columns (i).stag_column_nk_pos AS stag_column_nk_pos
                          FROM DUAL) src
                    ON (trg.stag_column_name = src.stag_column_name
                    AND trg.stag_object_id = r_obj.stag_object_id)
            WHEN MATCHED THEN
               UPDATE SET trg.stag_column_pos = src.stag_column_pos
                        , trg.stag_column_def = src.stag_column_def
                        , trg.stag_column_nk_pos = src.stag_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT     (
                             trg.stag_object_id
                           , trg.stag_column_pos
                           , trg.stag_column_name
                           , trg.stag_column_def
                           , trg.stag_column_nk_pos
                          )
                   VALUES (
                             r_obj.stag_object_id
                           , src.stag_column_pos
                           , src.stag_column_name
                           , src.stag_column_def
                           , src.stag_column_nk_pos
                          );
         END LOOP;

         COMMIT;
      END LOOP;

      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Check column changes'
       , 'Finish'
      );
   END;

   PROCEDURE prc_set_object_properties
   IS
   BEGIN
      -- Select all objects
      FOR r_obj IN (  SELECT stag_object_id
                           , stag_object_name
                           , stag_view_stage2_name
                           , CASE
                                WHEN root_cnt > 1 THEN
                                      SUBSTR (
                                         stag_object_root
                                       , 1
                                       , 25
                                      )
                                   || root_rank
                                ELSE
                                   stag_object_root
                             END
                                AS stag_object_root
                        FROM (SELECT t.*
                                   , COUNT (0) OVER (PARTITION BY stag_object_root) AS root_cnt
                                   , ROW_NUMBER () OVER (PARTITION BY stag_object_root ORDER BY stag_object_name) AS root_rank
                                FROM (SELECT o.stag_object_id
                                           , o.stag_object_name
                                           , SUBSTR (
                                                   CASE
                                                      WHEN s.stag_source_prefix IS NOT NULL THEN
                                                            s.stag_source_prefix
                                                         || '_'
                                                   END
                                                || o.stag_object_name
                                              , 1
                                              , 30
                                             )
                                                AS stag_view_stage2_name
                                           , SUBSTR (
                                                   CASE
                                                      WHEN s.stag_source_prefix IS NOT NULL THEN
                                                            s.stag_source_prefix
                                                         || '_'
                                                   END
                                                || o.stag_object_name
                                              , 1
                                              , 26
                                             )
                                                AS stag_object_root
                                        FROM p#frm#stag_source_t s
                                           , p#frm#stag_object_t o
                                       WHERE s.stag_source_id = o.stag_source_id) t)
                    ORDER BY stag_object_id) LOOP
         UPDATE p#frm#stag_object_t
            SET stag_object_root = r_obj.stag_object_root
              , stag_src_table_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_tab_source
              , stag_dupl_table_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_tab_dupl
              , stag_diff_table_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_tab_diff
              , stag_diff_nk_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_nk_diff
              , stag_stage_table_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_tab_stag
              , stag_hist_table_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_tab_hist
              , stag_hist_nk_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_nk_hist
              , stag_hist_view_name = r_obj.stag_view_stage2_name
              , stag_hist_fbda_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_view_fbda
              , stag_package_name =
                      r_obj.stag_object_root
                   || '_'
                   || p#frm#stag_param.c_vc_suffix_package
          WHERE stag_object_id = r_obj.stag_object_id;

         COMMIT;
      END LOOP;
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: stag_meta-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_meta/stag_meta-impl.sql $';
END p#frm#stag_meta;