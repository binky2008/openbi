CREATE OR REPLACE PACKAGE BODY stg_meta
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-23 17:56:23 +0200 (Mo, 23 Jul 2012) $
    * $Revision: 3021 $
    * $Id: stg_meta-impl.sql 3021 2012-07-23 15:56:23Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_meta/stg_meta-impl.sql $
    */
   -- List of object suffixes
   c_vc_suffix_table_source   type.vc_max_plsql := 'SRC';
   c_vc_suffix_table_dupl     type.vc_max_plsql := 'DUP';
   c_vc_suffix_table_diff     type.vc_max_plsql := 'DIF';
   c_vc_suffix_table_stage1   type.vc_max_plsql := 'ST1';
   c_vc_suffix_table_stage2   type.vc_max_plsql := 'ST2';
   c_vc_suffix_view_history   type.vc_max_plsql := 'H';
   c_vc_suffix_nk_diff        type.vc_max_plsql := 'NKD';
   c_vc_suffix_nk_stage2      type.vc_max_plsql := 'NK';
   c_vc_suffix_package        type.vc_max_plsql := 'PKG';

   FUNCTION fct_get_column_list (
      p_vc_object_id     IN   NUMBER
    , p_vc_column_type   IN   VARCHAR2
    , p_vc_list_type     IN   VARCHAR2
    , p_vc_alias1        IN   VARCHAR2 DEFAULT NULL
    , p_vc_alias2        IN   VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2
   IS
      l_vc_list   type.vc_max_plsql;
   BEGIN
      -- Build list of columns
      FOR r_col IN (SELECT   stg_column_name
                        FROM stg_column_t
                       WHERE stg_object_id = p_vc_object_id
                         AND stg_column_edwh_flag = 1
                         AND (   p_vc_column_type = 'ALL'
                              OR (    p_vc_column_type = 'PK'
                                  AND stg_column_nk_pos IS NOT NULL)
                              OR (    p_vc_column_type = 'NPK'
                                  AND stg_column_nk_pos IS NULL))
                    ORDER BY stg_column_nk_pos
                           , stg_column_pos)
      LOOP
         l_vc_list    :=
               l_vc_list
            || CHR (10)
            || CASE p_vc_list_type
                  WHEN 'LIST_SIMPLE'
                     THEN r_col.stg_column_name || ', '
                  WHEN 'LIST_ALIAS'
                     THEN p_vc_alias1 || '.' || r_col.stg_column_name || ', '
                  WHEN 'SET_ALIAS'
                     THEN p_vc_alias1 || '.' || r_col.stg_column_name || ' = ' || p_vc_alias2 || '.' || r_col.stg_column_name || ', '
                  WHEN 'LIST_NVL2'
                     THEN    'NVL2 ('
                          || p_vc_alias1
                          || '.rowid, '
                          || p_vc_alias1
                          || '.'
                          || r_col.stg_column_name
                          || ', '
                          || p_vc_alias2
                          || '.'
                          || r_col.stg_column_name
                          || ') AS '
                          || r_col.stg_column_name
                          || ', '
                  WHEN 'AND_NOTNULL'
                     THEN r_col.stg_column_name || ' IS NOT NULL AND '
                  WHEN 'AND_ALIAS'
                     THEN p_vc_alias1 || '.' || r_col.stg_column_name || ' = ' || p_vc_alias2 || '.' || r_col.stg_column_name || ' AND '
                  WHEN 'OR_DECODE'
                     THEN 'DECODE (' || p_vc_alias1 || '.' || r_col.stg_column_name || ', ' || p_vc_alias2 || '.' || r_col.stg_column_name || ', 0, 1) = 1 OR '
               END;
      END LOOP;

      IF p_vc_list_type IN ('LIST_SIMPLE', 'LIST_ALIAS', 'LIST_NVL2', 'SET_ALIAS')
      THEN
         l_vc_list    := RTRIM (l_vc_list, ', ');
      ELSIF p_vc_list_type IN ('AND_NOTNULL', 'AND_ALIAS')
      THEN
         l_vc_list    := SUBSTR (l_vc_list
                               , 1
                               , LENGTH (l_vc_list) - 5
                                );
      ELSIF p_vc_list_type = 'OR_DECODE'
      THEN
         l_vc_list    := SUBSTR (l_vc_list
                               , 1
                               , LENGTH (l_vc_list) - 4
                                );
      END IF;

      RETURN l_vc_list;
   END fct_get_column_list;

   PROCEDURE prc_stat_type_ins (
      p_vc_type_code   IN   VARCHAR2
    , p_vc_type_name   IN   VARCHAR2
    , p_vc_type_desc   IN   VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO stg_stat_type_t trg
         USING (SELECT p_vc_type_code AS type_code
                     , p_vc_type_name AS type_name
                     , p_vc_type_desc AS type_desc
                  FROM DUAL) src
         ON (trg.stg_stat_type_code = src.type_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_stat_type_name = src.type_name, trg.stg_stat_type_desc = src.type_desc
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_stat_type_code, trg.stg_stat_type_name, trg.stg_stat_type_desc)
            VALUES (src.type_code, src.type_name, src.type_desc);
      COMMIT;
   END prc_stat_type_ins;

   PROCEDURE prc_source_ins (
      p_vc_source_code      IN   VARCHAR2
    , p_vc_source_prefix    IN   VARCHAR2
    , p_vc_source_name      IN   VARCHAR2
    , p_vc_stage_owner      IN   VARCHAR2
    , p_vc_ts_stg1_data     IN   VARCHAR2
    , p_vc_ts_stg1_indx     IN   VARCHAR2
    , p_vc_ts_stg2_data     IN   VARCHAR2
    , p_vc_ts_stg2_indx     IN   VARCHAR2
    , p_vc_fb_archive       IN   VARCHAR2 DEFAULT NULL
    , p_vc_bodi_ds          IN   VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds   IN   VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      MERGE INTO stg_source_t trg
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
         ON (trg.stg_source_code = src.source_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_source_prefix = src.source_prefix, trg.stg_source_name = src.source_name, trg.stg_owner = src.stage_owner, trg.stg_ts_stg1_data = src.ts_stg1_data
                 , trg.stg_ts_stg1_indx = src.ts_stg1_indx, trg.stg_ts_stg2_data = src.ts_stg2_data, trg.stg_ts_stg2_indx = src.ts_stg2_indx
                 , trg.stg_fb_archive = src.fb_archive, trg.stg_bodi_ds = src.bodi_ds, trg.stg_source_bodi_ds = src.source_bodi_ds
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_source_code, trg.stg_source_prefix, trg.stg_source_name, trg.stg_owner, trg.stg_ts_stg1_data, trg.stg_ts_stg1_indx
                  , trg.stg_ts_stg2_data, trg.stg_ts_stg2_indx, trg.stg_fb_archive, trg.stg_bodi_ds, trg.stg_source_bodi_ds)
            VALUES (src.source_code, src.source_prefix, src.source_name, src.stage_owner, src.ts_stg1_data, src.ts_stg1_indx, src.ts_stg2_data, src.ts_stg2_indx, src.source_bodi_ds, src.bodi_ds
                  , src.source_bodi_ds);
      COMMIT;
   END prc_source_ins;

   PROCEDURE prc_source_del (
      p_vc_source_code   IN   VARCHAR2
    , p_b_cascade        IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_n_source_id   NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_cnt
        FROM stg_source_t
       WHERE stg_source_code = p_vc_source_code;

      IF l_n_cnt > 0
      THEN
         -- Get the key object id
         SELECT stg_source_id
           INTO l_n_source_id
           FROM stg_source_t
          WHERE stg_source_code = p_vc_source_code;

         IF NOT p_b_cascade
         THEN
            SELECT COUNT (*)
              INTO l_n_cnt
              FROM stg_object_t
             WHERE stg_source_id = l_n_source_id;

            IF l_n_cnt > 0
            THEN
               raise_application_error (-20001, 'Cannot delete source with objects');
            END IF;
         END IF;

         -- Delete children objects
         FOR r_obj IN (SELECT stg_object_name
                         FROM stg_object_t
                        WHERE stg_source_id = l_n_source_id)
         LOOP
            prc_object_del (p_vc_source_code
                          , r_obj.stg_object_name
                          , p_b_cascade
                           );
         END LOOP;

         DELETE      stg_source_db_t
               WHERE stg_source_id = l_n_source_id;

         DELETE      stg_source_t
               WHERE stg_source_code = p_vc_source_code;

         COMMIT;
      END IF;
   END prc_source_del;

   PROCEDURE prc_source_db_ins (
      p_vc_source_code          IN   VARCHAR2
    , p_vc_distribution_code    IN   VARCHAR2
    , p_vc_source_db_link       IN   VARCHAR2
    , p_vc_source_owner         IN   VARCHAR2
    , p_vc_source_db_jdbcname   IN   VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds       IN   VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      MERGE INTO stg_source_db_t trg
         USING (SELECT stg_source_id
                     , p_vc_distribution_code AS distribution_code
                     , p_vc_source_db_link AS source_db_link
                     , p_vc_source_db_jdbcname AS source_db_jdbcname
                     , p_vc_source_owner AS source_owner
                     , p_vc_source_bodi_ds AS source_bodi_ds
                  FROM stg_source_t
                 WHERE stg_source_code = p_vc_source_code) src
         ON (    trg.stg_source_id = src.stg_source_id
             AND trg.stg_distribution_code = src.distribution_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_source_db_link = src.source_db_link, trg.stg_source_db_jdbcname = src.source_db_jdbcname, trg.stg_source_owner = src.source_owner
                 , trg.stg_source_bodi_ds = src.source_bodi_ds
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_source_id, trg.stg_distribution_code, trg.stg_source_db_link, trg.stg_source_db_jdbcname, trg.stg_source_owner, trg.stg_source_bodi_ds)
            VALUES (src.stg_source_id, src.distribution_code, src.source_db_link, src.source_db_jdbcname, src.source_owner, src.source_bodi_ds);
      COMMIT;
   END prc_source_db_ins;

   PROCEDURE prc_object_ins (
      p_vc_source_code        IN   VARCHAR2
    , p_vc_object_name        IN   VARCHAR2
    , p_n_parallel_degree     IN   NUMBER DEFAULT NULL
    , p_vc_filter_clause      IN   VARCHAR2 DEFAULT NULL
    , p_vc_partition_clause   IN   VARCHAR2 DEFAULT NULL
    , p_vc_fbda_flag          IN   NUMBER DEFAULT NULL
    , p_vc_increment_buffer   IN   NUMBER DEFAULT NULL
    , p_vc_std_load_modus     IN   VARCHAR2 DEFAULT NULL
   )
   IS
      l_vc_table_comment   type.vc_max_plsql;
   BEGIN
      -- Set object
      MERGE INTO stg_object_t trg
         USING (SELECT stg_source_id
                     , p_vc_object_name AS object_name
                     , p_n_parallel_degree AS parallel_degree
                     , p_vc_filter_clause AS filter_clause
                     , p_vc_partition_clause AS partition_clause
                     , p_vc_fbda_flag AS fbda_flag
                     , p_vc_increment_buffer AS increment_buffer
                     , p_vc_std_load_modus AS std_load_modus
                  FROM stg_source_t
                 WHERE stg_source_code = p_vc_source_code) src
         ON (    trg.stg_source_id = src.stg_source_id
             AND trg.stg_object_name = src.object_name)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_parallel_degree = parallel_degree, trg.stg_filter_clause = filter_clause, trg.stg_partition_clause = partition_clause
                 , trg.stg_fbda_flag = src.fbda_flag, trg.stg_increment_buffer = src.increment_buffer, trg.stg_std_load_modus = src.std_load_modus
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_source_id, trg.stg_object_name, trg.stg_parallel_degree, trg.stg_filter_clause, trg.stg_partition_clause, trg.stg_fbda_flag
                  , trg.stg_increment_buffer, trg.stg_std_load_modus)
            VALUES (src.stg_source_id, src.object_name, src.parallel_degree, src.filter_clause, src.partition_clause, src.fbda_flag, src.increment_buffer, src.std_load_modus);
      COMMIT;

      -- Get object comment from source
      FOR r_obj IN (SELECT   stg_source_db_link
                           , stg_source_owner
                           , stg_object_id
                           , stg_object_name
                        FROM (SELECT d.stg_source_db_link
                                   , d.stg_source_owner
                                   , o.stg_object_id
                                   , o.stg_object_name
                                   , ROW_NUMBER () OVER (PARTITION BY o.stg_object_id ORDER BY d.stg_source_db_id) AS source_db_order
                                FROM stg_source_t s
                                   , stg_source_db_t d
                                   , stg_object_t o
                               WHERE s.stg_source_id = d.stg_source_id
                                 AND s.stg_source_id = o.stg_source_id
                                 AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                                 AND p_vc_object_name IN (o.stg_object_name, 'ALL'))
                       WHERE source_db_order = 1
                    ORDER BY stg_object_id)
      LOOP
         UPDATE stg_object_t
            SET stg_object_comment = dict.fct_get_table_comment (r_obj.stg_source_db_link
                                                                            , r_obj.stg_source_owner
                                                                            , r_obj.stg_object_name
                                                                             )
          WHERE stg_object_id = r_obj.stg_object_id;
      END LOOP;
   END prc_object_ins;

   PROCEDURE prc_object_del (
      p_vc_source_code   IN   VARCHAR2
    , p_vc_object_name   IN   VARCHAR2
    , p_b_cascade        IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_n_object_id   NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_cnt
        FROM stg_source_t s
           , stg_object_t o
       WHERE s.stg_source_id = o.stg_source_id
         AND s.stg_source_code = p_vc_source_code
         AND o.stg_object_name = p_vc_object_name;

      IF l_n_cnt > 0
      THEN
         -- Get the key object id
         SELECT o.stg_object_id
           INTO l_n_object_id
           FROM stg_source_t s
              , stg_object_t o
          WHERE s.stg_source_id = o.stg_source_id
            AND s.stg_source_code = p_vc_source_code
            AND o.stg_object_name = p_vc_object_name;

         IF NOT p_b_cascade
         THEN
            SELECT COUNT (*)
              INTO l_n_cnt
              FROM stg_column_t
             WHERE stg_object_id = l_n_object_id;

            IF l_n_cnt > 0
            THEN
               raise_application_error (-20001, 'Cannot delete object with columns');
            END IF;
         END IF;

         DELETE      stg_column_t
               WHERE stg_object_id = l_n_object_id;

         DELETE      stg_object_t
               WHERE stg_object_id = l_n_object_id;

         COMMIT;
      END IF;
   END prc_object_del;

   PROCEDURE prc_column_ins (
      p_vc_source_code       IN   VARCHAR2
    , p_vc_object_name       IN   VARCHAR2
    , p_vc_column_name       IN   VARCHAR2
    , p_vc_column_name_map   IN   VARCHAR2 DEFAULT NULL
    , p_vc_column_def        IN   VARCHAR2 DEFAULT NULL
    , p_n_column_pos         IN   NUMBER DEFAULT NULL
    , p_n_column_nk_pos      IN   NUMBER DEFAULT NULL
    , p_n_column_incr_flag   IN   NUMBER DEFAULT 0
    , p_n_column_hist_flag   IN   NUMBER DEFAULT 1
    , p_n_column_edwh_flag   IN   NUMBER DEFAULT 1
   )
   IS
   BEGIN
      MERGE INTO stg_column_t trg
         USING (SELECT o.stg_object_id
                     , p_vc_object_name AS object_name
                     , p_vc_column_name AS column_name
                     , p_vc_column_name_map AS column_name_map
                     , p_vc_column_def AS column_def
                     , p_n_column_pos AS column_pos
                     , p_n_column_nk_pos AS column_nk_pos
                     , p_n_column_incr_flag AS column_incr_flag
                     , p_n_column_hist_flag AS column_hist_flag
                     , p_n_column_edwh_flag AS column_edwh_flag
                  FROM stg_source_t s
                     , stg_object_t o
                 WHERE s.stg_source_id = o.stg_source_id
                   AND s.stg_source_code = p_vc_source_code
                   AND o.stg_object_name = p_vc_object_name) src
         ON (    trg.stg_object_id = src.stg_object_id
             AND trg.stg_column_name = src.column_name)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_column_name_map = NVL (src.column_name_map, trg.stg_column_name_map), trg.stg_column_def = NVL (src.column_def, trg.stg_column_def)
                 , trg.stg_column_pos = NVL (src.column_pos, trg.stg_column_pos), trg.stg_column_nk_pos = NVL (src.column_nk_pos, trg.stg_column_nk_pos)
                 , trg.stg_column_incr_flag = NVL (src.column_incr_flag, trg.stg_column_incr_flag)
                 , trg.stg_column_hist_flag = NVL (src.column_hist_flag, trg.stg_column_hist_flag)
                 , trg.stg_column_edwh_flag = NVL (src.column_edwh_flag, trg.stg_column_edwh_flag)
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_object_id, trg.stg_column_name, trg.stg_column_name_map, trg.stg_column_def, trg.stg_column_pos, trg.stg_column_nk_pos
                  , trg.stg_column_incr_flag, trg.stg_column_hist_flag, trg.stg_column_edwh_flag)
            VALUES (src.stg_object_id, src.column_name, src.column_name_map, src.column_def, src.column_pos, src.column_nk_pos, src.column_incr_flag, src.column_hist_flag, src.column_edwh_flag);
      COMMIT;
   END prc_column_ins;

   PROCEDURE prc_column_del (
      p_vc_source_code   IN   VARCHAR2
    , p_vc_object_name   IN   VARCHAR2
    , p_vc_column_name   IN   VARCHAR2
   )
   IS
   BEGIN
      DELETE      stg_column_t
            WHERE stg_object_id = (SELECT o.stg_object_id
                                           FROM stg_source_t s
                                              , stg_object_t o
                                          WHERE s.stg_source_id = o.stg_source_id
                                            AND s.stg_source_code = p_vc_source_code
                                            AND o.stg_object_name = p_vc_object_name)
              AND stg_column_name = p_vc_column_name;

      COMMIT;
   END prc_column_del;

   PROCEDURE prc_column_import (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   )
   IS
      l_n_pk_pos_min   NUMBER;
   BEGIN
      trc.log_info ('Prepare metadata', 'Start');

      FOR r_obj IN (SELECT stg_object_id
                         , stg_object_name
                         , stg_owner
                         , stg_source_owner
                         , stg_source_db_link
                      FROM (SELECT o.stg_object_id
                                 , o.stg_object_name
                                 , s.stg_owner
                                 , d.stg_source_owner
                                 , d.stg_source_db_link
                                 , ROW_NUMBER () OVER (PARTITION BY o.stg_object_id ORDER BY d.stg_source_db_id) AS db_rank
                              FROM stg_object_t o
                                 , stg_source_t s
                                 , stg_source_db_t d
                             WHERE o.stg_source_id = s.stg_source_id
                               AND s.stg_source_id = d.stg_source_id
                               AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                               AND p_vc_object_name IN (o.stg_object_name, 'ALL'))
                     WHERE db_rank = 1)
      LOOP
         -- Load metadata in the temp table
         dict.prc_import_metadata (r_obj.stg_source_db_link
                                        , r_obj.stg_source_owner
                                        ,    r_obj.stg_object_name
                                          || CASE
                                                WHEN r_obj.stg_source_db_link IS NULL
                                                AND r_obj.stg_source_owner = r_obj.stg_owner
                                                   THEN '_' || c_vc_suffix_table_source
                                             END
                                        , 'stg_column_tmp'
                                        , NULL
                                        , p_b_check_dependencies
                                         );

         SELECT NVL (MIN (stg_column_nk_pos), 0)
           INTO l_n_pk_pos_min
           FROM stg_column_tmp;

         UPDATE stg_object_t
            SET stg_source_nk_flag = CASE
                                             WHEN l_n_pk_pos_min = 0
                                                THEN 0
                                             ELSE 1
                                          END
          WHERE stg_object_id = r_obj.stg_object_id;

         MERGE INTO stg_column_t trg
            USING (SELECT stg_column_name
                        , stg_column_comment
                        , stg_column_pos
                        , stg_column_def
                        , stg_column_nk_pos
                     FROM stg_column_tmp) src
            ON (trg.stg_column_name = src.stg_column_name
            AND trg.stg_object_id = r_obj.stg_object_id)
            WHEN MATCHED THEN
               UPDATE
                  SET trg.stg_column_pos = src.stg_column_pos, trg.stg_column_def = src.stg_column_def, trg.stg_column_comment = src.stg_column_comment
                    , trg.stg_column_nk_pos = src.stg_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT (trg.stg_object_id, trg.stg_column_pos, trg.stg_column_name, trg.stg_column_comment, trg.stg_column_def, trg.stg_column_nk_pos
                     , trg.stg_column_edwh_flag)
               VALUES (r_obj.stg_object_id, src.stg_column_pos, src.stg_column_name, src.stg_column_comment, src.stg_column_def, src.stg_column_nk_pos, 1);
         COMMIT;
      END LOOP;

      trc.log_info ('Prepare metadata', 'Finish');
   END prc_column_import;

   PROCEDURE prc_column_import_from_stg1 (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   )
   IS
      l_n_pk_pos_min   NUMBER;
   BEGIN
      trc.LOG ('Prepare metadata', 'Start');
      prc_set_object_properties;

      FOR r_obj IN (SELECT stg_owner
                         , stg_object_id
                         , stg_object_name
                         , stg_stg1_table_name
                      FROM stg_object_t o
                         , stg_source_t s
                     WHERE o.stg_source_id = s.stg_source_id
                       AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                       AND p_vc_object_name IN (o.stg_object_name, 'ALL'))
      LOOP
         -- Load metadata in the temp table
         dict.prc_import_metadata (NULL
                                        , r_obj.stg_owner
                                        , r_obj.stg_stg1_table_name
                                        , 'stg_column_tmp'
                                        , NULL
                                        , p_b_check_dependencies
                                         );

         SELECT NVL (MIN (stg_column_nk_pos), 0)
           INTO l_n_pk_pos_min
           FROM stg_column_tmp;

         UPDATE stg_object_t
            SET stg_source_nk_flag = CASE
                                             WHEN l_n_pk_pos_min = 0
                                                THEN 0
                                             ELSE 1
                                          END
          WHERE stg_object_id = r_obj.stg_object_id;

         MERGE INTO stg_column_t trg
            USING (SELECT stg_column_name
                        , stg_column_comment
                        , stg_column_pos
                        , stg_column_def
                        , stg_column_nk_pos
                     FROM stg_column_tmp) src
            ON (trg.stg_column_name = src.stg_column_name
            AND trg.stg_object_id = r_obj.stg_object_id)
            WHEN MATCHED THEN
               UPDATE
                  SET trg.stg_column_pos = src.stg_column_pos, trg.stg_column_def = src.stg_column_def, trg.stg_column_comment = src.stg_column_comment
                    , trg.stg_column_nk_pos = src.stg_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT (trg.stg_object_id, trg.stg_column_pos, trg.stg_column_name, trg.stg_column_comment, trg.stg_column_def, trg.stg_column_nk_pos
                     , trg.stg_column_edwh_flag)
               VALUES (r_obj.stg_object_id, src.stg_column_pos, src.stg_column_name, src.stg_column_comment, src.stg_column_def, src.stg_column_nk_pos, 1);
         COMMIT;
      END LOOP;

      trc.LOG ('Prepare metadata', 'Finish');
   END prc_column_import_from_stg1;

   PROCEDURE prc_check_column_changes (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   )
   IS
   BEGIN
      trc.log_info ('Check column changes', 'Start');

      FOR r_obj IN (SELECT   stg_source_id
                           , stg_source_code
                           , stg_source_db_link
                           , stg_source_owner
                           , stg_owner
                           , stg_object_id
                           , stg_object_name
                           , stg_stg1_table_name
                        FROM (SELECT s.stg_source_id
                                   , s.stg_source_code
                                   , d.stg_source_db_link
                                   , d.stg_source_owner
                                   , s.stg_owner
                                   , o.stg_object_id
                                   , o.stg_object_name
                                   , o.stg_stg1_table_name
                                   , ROW_NUMBER () OVER (PARTITION BY o.stg_object_id ORDER BY d.stg_source_db_id) AS source_db_order
                                FROM stg_source_t s
                                   , stg_source_db_t d
                                   , stg_object_t o
                               WHERE s.stg_source_id = d.stg_source_id
                                 AND s.stg_source_id = o.stg_source_id
                                 AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                                 AND p_vc_object_name IN (o.stg_object_name, 'ALL'))
                       WHERE source_db_order = 1
                    ORDER BY stg_object_id)
      LOOP
         -- Load metadata in the temp table
         dict.prc_import_metadata (r_obj.stg_source_db_link
                                        , r_obj.stg_source_owner
                                        ,    r_obj.stg_object_name
                                          || CASE
                                                WHEN r_obj.stg_source_db_link IS NULL
                                                AND r_obj.stg_source_owner = r_obj.stg_owner
                                                   THEN '_' || c_vc_suffix_table_source
                                             END
                                        , 'stg_column_tmp'
                                        , NULL
                                        , p_b_check_dependencies
                                         );

         DELETE      stg_column_check_t
               WHERE stg_object_id = r_obj.stg_object_id;

         MERGE INTO stg_column_check_t trg
            USING (SELECT stg_column_name
                        , stg_column_comment
                        , stg_column_pos
                        , stg_column_def
                        , stg_column_nk_pos
                     FROM stg_column_tmp) src
            ON (trg.stg_column_name = src.stg_column_name
            AND trg.stg_object_id = r_obj.stg_object_id)
            WHEN MATCHED THEN
               UPDATE
                  SET trg.stg_column_pos = src.stg_column_pos, trg.stg_column_def = src.stg_column_def, trg.stg_column_nk_pos = src.stg_column_nk_pos
            WHEN NOT MATCHED THEN
               INSERT (trg.stg_object_id, trg.stg_column_pos, trg.stg_column_name, trg.stg_column_def, trg.stg_column_nk_pos)
               VALUES (r_obj.stg_object_id, src.stg_column_pos, src.stg_column_name, src.stg_column_def, src.stg_column_nk_pos);
         COMMIT;
      END LOOP;

      trc.log_info ('Check column changes', 'Finish');
   END;

   PROCEDURE prc_set_object_properties
   IS
   BEGIN
      -- Select all objects
      FOR r_obj IN (SELECT   stg_object_id
                           , stg_object_name
                           , stg_view_stage2_name
                           , CASE
                                WHEN root_cnt > 1
                                   THEN SUBSTR (stg_object_root
                                              , 1
                                              , 25
                                               ) || root_rank
                                ELSE stg_object_root
                             END AS stg_object_root
                        FROM (SELECT t.*
                                   , COUNT (0) OVER (PARTITION BY stg_object_root) AS root_cnt
                                   , ROW_NUMBER () OVER (PARTITION BY stg_object_root ORDER BY stg_object_name) AS root_rank
                                FROM (SELECT o.stg_object_id
                                           , o.stg_object_name
                                           , SUBSTR (CASE
                                                        WHEN s.stg_source_prefix IS NOT NULL
                                                           THEN s.stg_source_prefix || '_'
                                                     END || o.stg_object_name
                                                   , 1
                                                   , 30
                                                    ) AS stg_view_stage2_name
                                           , SUBSTR (CASE
                                                        WHEN s.stg_source_prefix IS NOT NULL
                                                           THEN s.stg_source_prefix || '_'
                                                     END || o.stg_object_name
                                                   , 1
                                                   , 26
                                                    ) AS stg_object_root
                                        FROM stg_source_t s
                                           , stg_object_t o
                                       WHERE s.stg_source_id = o.stg_source_id) t)
                    ORDER BY stg_object_id)
      LOOP
         UPDATE stg_object_t
            SET stg_object_root = r_obj.stg_object_root
              , stg_src_table_name = r_obj.stg_object_root || '_' || c_vc_suffix_table_source
              , stg_dupl_table_name = r_obj.stg_object_root || '_' || c_vc_suffix_table_dupl
              , stg_diff_table_name = r_obj.stg_object_root || '_' || c_vc_suffix_table_diff
              , stg_diff_nk_name = r_obj.stg_object_root || '_' || c_vc_suffix_nk_diff
              , stg_stg1_table_name = r_obj.stg_object_root || '_' || c_vc_suffix_table_stage1
              , stg_stg2_table_name = r_obj.stg_object_root || '_' || c_vc_suffix_table_stage2
              , stg_stg2_nk_name = r_obj.stg_object_root || '_' || c_vc_suffix_nk_stage2
              , stg_stg2_view_name = r_obj.stg_view_stage2_name
              , stg_stg2_hist_name = r_obj.stg_object_root || '_' || c_vc_suffix_view_history
              , stg_package_name = r_obj.stg_object_root || '_' || c_vc_suffix_package
          WHERE stg_object_id = r_obj.stg_object_id;

         COMMIT;
      END LOOP;
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: stg_meta-impl.sql 3021 2012-07-23 15:56:23Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_meta/stg_meta-impl.sql $';
END stg_meta;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('stg_meta'
                                 , 'stg_meta'
                                 , TRUE
                                  );
END;
/

SHOW errors