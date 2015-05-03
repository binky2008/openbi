CREATE OR REPLACE PACKAGE BODY p#frm#mesr
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: mesr-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/mesr/mesr-impl.sql $
   */
   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * Key value type
   */
   TYPE r_keyvalue IS RECORD (
      keyfigure     VARCHAR2 (100)
    , resultvalue   NUMBER
   );

   TYPE t_keyvalue IS TABLE OF r_keyvalue;

   FUNCTION fct_exec_verify (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_n_exec_value        IN NUMBER
   )
      RETURN BOOLEAN
   IS
      l_vc_prc_name         t_object_name := 'FCT_EXEC_VERIFY';
      l_n_keyfigure_id      NUMBER;
      l_vc_threshold_type   CHAR (1);
      l_n_threshold_min     NUMBER;
      l_n_threshold_max     NUMBER;
      l_n_result_previous   NUMBER;
      l_n_increment         NUMBER;
      l_n_cnt               NUMBER := 0;
      l_b_success           BOOLEAN := TRUE;
   BEGIN
      SELECT MIN (k.mesr_keyfigure_id)
           , MIN (t.mesr_threshold_type)
           , MIN (t.mesr_threshold_min)
           , MAX (t.mesr_threshold_max)
        INTO l_n_keyfigure_id
           , l_vc_threshold_type
           , l_n_threshold_min
           , l_n_threshold_max
        FROM p#frm#mesr_query_t s
           , p#frm#mesr_keyfigure_t k
           , p#frm#mesr_threshold_t t
       WHERE s.mesr_query_id = k.mesr_query_id
         AND t.mesr_keyfigure_id = k.mesr_keyfigure_id
         AND s.mesr_query_code = p_vc_query_code
         AND k.mesr_keyfigure_code = p_vc_keyfigure_code
         AND t.mesr_threshold_from <= SYSDATE
         AND SYSDATE < t.mesr_threshold_to;

      p#frm#trac.log_sub_info (
            'Key figure '
         || p_vc_keyfigure_code
         || ' type '
         || l_vc_threshold_type
         || ' threshold = '
         || l_n_threshold_min
         || ' - '
         || l_n_threshold_max
       , 'VERIFYING'
      );

      IF l_vc_threshold_type = 'A' THEN
         IF l_n_threshold_min IS NOT NULL
        AND l_n_threshold_max IS NOT NULL
        AND p_n_exec_value NOT BETWEEN l_n_threshold_min AND l_n_threshold_max THEN
            l_b_success := FALSE;
            p#frm#trac.log_sub_info (
                  'Result '
               || p_n_exec_value
               || ' not ok'
             , 'RESULT NOT OK'
            );
         ELSE
            p#frm#trac.log_sub_info (
                  'Result '
               || p_n_exec_value
               || ' ok'
             , 'RESULT OK'
            );
         END IF;
      ELSIF l_vc_threshold_type = 'I' THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM p#frm#mesr_exec_t
          WHERE mesr_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0 THEN
            SELECT MAX (NVL (mesr_exec_result_value, 0))
              INTO l_n_result_previous
              FROM (SELECT mesr_exec_id
                         , mesr_exec_result_value
                         , MAX (mesr_exec_id) OVER (PARTITION BY mesr_keyfigure_id) AS mesr_exec_last
                      FROM p#frm#mesr_exec_t
                     WHERE mesr_keyfigure_id = l_n_keyfigure_id)
             WHERE mesr_exec_id = mesr_exec_last;

            p#frm#trac.log_sub_info (
                  'Previous result = '
               || l_n_result_previous
             , 'VERIFYING INCREMENT'
            );

            IF l_n_result_previous > 0 THEN
               l_n_increment :=
                    (  p_n_exec_value
                     - l_n_result_previous)
                  / l_n_result_previous;

               IF l_n_threshold_min IS NOT NULL
              AND l_n_threshold_max IS NOT NULL
              AND l_n_increment NOT BETWEEN l_n_threshold_min AND l_n_threshold_max THEN
                  l_b_success := FALSE;
                  p#frm#trac.log_sub_info (
                        'Increment '
                     || l_n_increment
                     || ' not ok'
                   , 'RESULT NOT OK'
                  );
               ELSE
                  p#frm#trac.log_sub_info (
                        'Increment '
                     || l_n_increment
                     || ' ok'
                   , 'RESULT OK'
                  );
               END IF;
            ELSE
               p#frm#trac.log_sub_info (
                     'Previous result = '
                  || l_n_result_previous
                , 'RESULT OK'
               );
            END IF;
         ELSE
            p#frm#trac.log_sub_info (
                  'Key figure '
               || p_vc_keyfigure_code
               || ' type '
               || l_vc_threshold_type
               || ' - no previous results available'
             , 'RESULT OK'
            );
         END IF;
      END IF;

      RETURN l_b_success;
   END fct_exec_verify;

   PROCEDURE prc_mesr_taxn_ins (
      p_vc_query_code      IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_mesr_TAXONOMY_INS';
   BEGIN
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Inserting in mesr_case_taxonomy_t'
      );

      MERGE INTO p#frm#mesr_taxn_t trg
           USING (SELECT mesr_query_id
                       , taxn_id
                    FROM p#frm#mesr_query_t c
                       , p#frm#taxn_t t
                   WHERE c.mesr_query_code = p_vc_query_code
                     AND t.taxn_code = p_vc_taxonomy_code) src
              ON (trg.mesr_query_id = src.mesr_query_id
              AND trg.taxn_id = src.taxn_id)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.mesr_query_id
                     , trg.taxn_id
                    )
             VALUES (
                       src.mesr_query_id
                     , src.taxn_id
                    );

      p#frm#trac.log_sub_info (
         l_vc_prc_name
       ,    SQL%ROWCOUNT
         || ' rows merged'
      );
      COMMIT;
   END prc_mesr_taxn_ins;

   PROCEDURE prc_mesr_taxn_del (
      p_vc_query_code      IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_CASE_TAXONOMY_DEL';
   BEGIN
      p#frm#trac.log_sub_info (
         'Deleting in mesr_case_taxonomy_t'
       , l_vc_prc_name
      );

      DELETE p#frm#mesr_taxn_t
       WHERE mesr_query_id = (SELECT mesr_query_id
                                FROM p#frm#mesr_query_t
                               WHERE mesr_query_code = p_vc_query_code)
         AND taxn_id = (SELECT taxn_id
                          FROM p#frm#taxn_t
                         WHERE taxn_code = p_vc_taxonomy_code);

      p#frm#trac.log_sub_info (
         l_vc_prc_name
       ,    SQL%ROWCOUNT
         || ' rows deleted'
      );
      COMMIT;
   END prc_mesr_taxn_del;

   PROCEDURE prc_query_ins (
      p_vc_query_code   IN VARCHAR2
    , p_vc_query_name   IN VARCHAR2
    , p_vc_query_sql    IN CLOB
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_query_INS';
   BEGIN
      MERGE INTO p#frm#mesr_query_t trg
           USING (SELECT p_vc_query_code AS query_code
                       , p_vc_query_name AS query_name
                       , p_vc_query_sql AS query_sql
                    FROM DUAL) src
              ON (trg.mesr_query_code = src.query_code)
      WHEN MATCHED THEN
         UPDATE SET trg.mesr_query_name = NVL (src.query_name, trg.mesr_query_name)
                  , trg.mesr_query_sql = NVL (src.query_sql, trg.mesr_query_sql)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.mesr_query_code
                     , trg.mesr_query_name
                     , trg.mesr_query_sql
                    )
             VALUES (
                       src.query_code
                     , src.query_name
                     , src.query_sql
                    );

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows merged'
       , l_vc_prc_name
      );
      COMMIT;
   END prc_query_ins;

   PROCEDURE prc_query_del (
      p_vc_query_code   IN VARCHAR2
    , p_b_cascade       IN BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_query_DEL';
      l_n_query_id    NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      -- Get the query id
      SELECT mesr_query_id
        INTO l_n_query_id
        FROM p#frm#mesr_query_t
       WHERE mesr_query_code = p_vc_query_code;

      IF NOT p_b_cascade THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM p#frm#mesr_keyfigure_t
          WHERE mesr_query_id = l_n_query_id;

         IF l_n_cnt > 0 THEN
            raise_application_error (
               -20001
             , 'Cannot delete query with key figures'
            );
         END IF;
      END IF;

      FOR r_key IN (SELECT mesr_keyfigure_code
                      FROM p#frm#mesr_keyfigure_t
                     WHERE mesr_query_id = l_n_query_id) LOOP
         prc_keyfigure_del (
            p_vc_query_code
          , r_key.mesr_keyfigure_code
          , p_b_cascade
         );
      END LOOP;

      DELETE p#frm#mesr_query_t
       WHERE mesr_query_id = l_n_query_id;

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows deleted'
       , l_vc_prc_name
      );
      COMMIT;
   END prc_query_del;

   PROCEDURE prc_keyfigure_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_vc_keyfigure_name   IN VARCHAR2
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_KEYFIGURE_INS';
   BEGIN
      MERGE INTO p#frm#mesr_keyfigure_t trg
           USING (SELECT s.mesr_query_id
                       , p_vc_keyfigure_code AS keyfigure_code
                       , p_vc_keyfigure_name AS keyfigure_name
                    FROM p#frm#mesr_query_t s
                   WHERE s.mesr_query_code = p_vc_query_code) src
              ON (trg.mesr_query_id = src.mesr_query_id
              AND trg.mesr_keyfigure_code = src.keyfigure_code)
      WHEN MATCHED THEN
         UPDATE SET trg.mesr_keyfigure_name = NVL (src.keyfigure_name, trg.mesr_keyfigure_name)
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.mesr_query_id
                     , trg.mesr_keyfigure_code
                     , trg.mesr_keyfigure_name
                    )
             VALUES (
                       src.mesr_query_id
                     , src.keyfigure_code
                     , src.keyfigure_name
                    );

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows merged'
       , l_vc_prc_name
      );
      COMMIT;
   END prc_keyfigure_ins;

   PROCEDURE prc_keyfigure_del (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_b_cascade           IN BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name      t_object_name := 'PRC_KEYFIGURE_DEL';
      l_n_keyfigure_id   NUMBER;
      l_n_cnt            NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.mesr_keyfigure_id
        INTO l_n_keyfigure_id
        FROM p#frm#mesr_query_t s
           , p#frm#mesr_keyfigure_t k
       WHERE s.mesr_query_id = k.mesr_query_id
         AND s.mesr_query_code = p_vc_query_code
         AND k.mesr_keyfigure_code = p_vc_keyfigure_code;

      IF NOT p_b_cascade THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM p#frm#mesr_exec_t
          WHERE mesr_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0 THEN
            raise_application_error (
               -20001
             , 'Cannot delete key figure with execution results'
            );
         END IF;
      END IF;

      DELETE p#frm#mesr_exec_t
       WHERE mesr_keyfigure_id = l_n_keyfigure_id;

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows deleted'
       , l_vc_prc_name
      );

      DELETE p#frm#mesr_threshold_t
       WHERE mesr_keyfigure_id = l_n_keyfigure_id;

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows deleted'
       , l_vc_prc_name
      );

      DELETE p#frm#mesr_keyfigure_t
       WHERE mesr_keyfigure_id = l_n_keyfigure_id;

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows deleted'
       , l_vc_prc_name
      );
      COMMIT;
   END prc_keyfigure_del;

   PROCEDURE prc_threshold_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_vc_threshold_type   IN VARCHAR2
    , p_n_threshold_min     IN NUMBER
    , p_n_threshold_max     IN NUMBER
    , p_d_threshold_from    IN DATE DEFAULT TO_DATE (
                                               '01011111'
                                             , 'ddmmyyyy'
                                            )
    , p_d_threshold_to      IN DATE DEFAULT TO_DATE (
                                               '09099999'
                                             , 'ddmmyyyy'
                                            )
   )
   IS
      l_vc_prc_name        t_object_name := 'PRC_THRESHOLD_INS';
      l_d_threshold_from   DATE
                              := NVL (
                                    p_d_threshold_from
                                  , TO_DATE (
                                       '01011111'
                                     , 'ddmmyyyy'
                                    )
                                 );
      l_d_threshold_to     DATE
                              := NVL (
                                    p_d_threshold_to
                                  , TO_DATE (
                                       '09099999'
                                     , 'ddmmyyyy'
                                    )
                                 );
      l_n_keyfigure_id     NUMBER;
      l_n_threshold_id     NUMBER;
      l_n_split_flag       NUMBER;
      l_n_split_min        NUMBER;
      l_n_split_max        NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.mesr_keyfigure_id
        INTO l_n_keyfigure_id
        FROM p#frm#mesr_query_t s
           , p#frm#mesr_keyfigure_t k
       WHERE s.mesr_query_id = k.mesr_query_id
         AND s.mesr_query_code = p_vc_query_code
         AND k.mesr_keyfigure_code = p_vc_keyfigure_code;

      IF l_n_keyfigure_id IS NOT NULL THEN
         -- Delete existing time slices if they reside between new boundary
         DELETE p#frm#mesr_threshold_t
          WHERE mesr_keyfigure_id = l_n_keyfigure_id
            AND mesr_threshold_from > l_d_threshold_from
            AND mesr_threshold_to < l_d_threshold_to;

         p#frm#trac.log_sub_info (
               SQL%ROWCOUNT
            || ' rows deleted'
          , l_vc_prc_name
         );

         -- If new slice inside existing then split
         INSERT INTO p#frm#mesr_threshold_t (
                        mesr_keyfigure_id
                      , mesr_threshold_type
                      , mesr_threshold_min
                      , mesr_threshold_max
                      , mesr_threshold_from
                      , mesr_threshold_to
                     )
            SELECT mesr_keyfigure_id
                 , mesr_threshold_type
                 , mesr_threshold_min
                 , mesr_threshold_max
                 , l_d_threshold_to
                 , mesr_threshold_to
              FROM p#frm#mesr_threshold_t
             WHERE mesr_keyfigure_id = l_n_keyfigure_id
               AND mesr_threshold_from < l_d_threshold_from
               AND mesr_threshold_to > l_d_threshold_to;

         p#frm#trac.log_sub_info (
               SQL%ROWCOUNT
            || ' rows inserted'
          , l_vc_prc_name
         );

         -- Update existing time slice where upper bound > new lower bound
         UPDATE p#frm#mesr_threshold_t
            SET mesr_threshold_to = l_d_threshold_from
          WHERE mesr_keyfigure_id = l_n_keyfigure_id
            AND mesr_threshold_from < l_d_threshold_from
            AND mesr_threshold_to > l_d_threshold_from;

         p#frm#trac.log_sub_info (
               SQL%ROWCOUNT
            || ' rows updated'
          , l_vc_prc_name
         );

         -- Update existing time slice where lower bound < new upper bound
         UPDATE p#frm#mesr_threshold_t
            SET mesr_threshold_from = l_d_threshold_to
          WHERE mesr_keyfigure_id = l_n_keyfigure_id
            AND mesr_threshold_to > l_d_threshold_to
            AND mesr_threshold_from < l_d_threshold_to;

         p#frm#trac.log_sub_info (
               SQL%ROWCOUNT
            || ' rows updated'
          , l_vc_prc_name
         );

            -- Update time slice with same boundary
            UPDATE p#frm#mesr_threshold_t
               SET mesr_threshold_type = p_vc_threshold_type
                 , mesr_threshold_min = p_n_threshold_min
                 , mesr_threshold_max = p_n_threshold_max
             WHERE mesr_keyfigure_id = l_n_keyfigure_id
               AND mesr_threshold_from = l_d_threshold_from
               AND mesr_threshold_to = l_d_threshold_to
         RETURNING mesr_threshold_id
              INTO l_n_threshold_id;

         p#frm#trac.log_sub_info (
               SQL%ROWCOUNT
            || ' rows updated'
          , l_vc_prc_name
         );

         IF l_n_threshold_id IS NULL THEN
            INSERT INTO p#frm#mesr_threshold_t (
                           mesr_keyfigure_id
                         , mesr_threshold_type
                         , mesr_threshold_min
                         , mesr_threshold_max
                         , mesr_threshold_from
                         , mesr_threshold_to
                        )
                 VALUES (
                           l_n_keyfigure_id
                         , p_vc_threshold_type
                         , p_n_threshold_min
                         , p_n_threshold_max
                         , l_d_threshold_from
                         , l_d_threshold_to
                        );

            p#frm#trac.log_sub_info (
                  SQL%ROWCOUNT
               || ' rows inserted'
             , l_vc_prc_name
            );
         END IF;

         COMMIT;
      END IF;
   END prc_threshold_ins;

   PROCEDURE prc_exec_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_n_result_value      IN NUMBER
    , p_vc_result_report    IN CLOB
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_EXEC_INS';
   BEGIN
      INSERT INTO p#frm#mesr_exec_t (
                     mesr_keyfigure_id
                   , mesr_exec_result_value
                   , mesr_exec_result_report
                  )
         SELECT k.mesr_keyfigure_id
              , p_n_result_value
              , p_vc_result_report
           FROM p#frm#mesr_query_t s
              , p#frm#mesr_keyfigure_t k
          WHERE s.mesr_query_id = k.mesr_query_id
            AND s.mesr_query_code = p_vc_query_code
            AND k.mesr_keyfigure_code = p_vc_keyfigure_code;

      p#frm#trac.log_sub_info (
            SQL%ROWCOUNT
         || ' rows inserted'
       , l_vc_prc_name
      );
      COMMIT;
   END prc_exec_ins;

   PROCEDURE prc_exec (
      p_vc_query_code          IN VARCHAR2 DEFAULT 'ALL'
    , p_b_exception_if_fails   IN BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name       t_object_name := 'PRC_EXEC';
      l_keyfigure         t_keyvalue;
      l_vc_query_table    VARCHAR2 (100);
      l_vc_stmt           VARCHAR2 (32000);
      l_vc_report         CLOB;
      l_vc_job_name       VARCHAR2 (100);
      l_n_gui             NUMBER;
      l_n_query_no        NUMBER;
      l_n_result          NUMBER;
      l_n_threshold_min   NUMBER;
      l_n_threshold_max   NUMBER;
      l_b_success         BOOLEAN := TRUE;
   BEGIN
      p#frm#trac.log_sub_info (
            'Execute case query '
         || p_vc_query_code
       , 'Query START'
      );
      p#frm#trac.log_sub_info (
            'Results will be stored as '
         || p_vc_storage_type
       ,    'STORAGE '
         || p_vc_storage_type
      );

      FOR r_query IN (  SELECT s.mesr_query_id
                             , s.mesr_query_code
                             , s.mesr_query_sql
                          FROM p#frm#mesr_query_t s
                         WHERE (p_vc_query_code IN (s.mesr_query_code, 'ALL')
                             OR p_vc_query_code IS NULL)
                      ORDER BY s.mesr_query_code) LOOP
         p#frm#trac.log_sub_info (
               'query '
            || r_query.mesr_query_code
          , 'query START'
         );

         BEGIN
            IF p_vc_storage_type = 'VALUE'
            OR p_vc_storage_type IS NULL THEN
               EXECUTE IMMEDIATE r_query.mesr_query_sql BULK COLLECT INTO l_keyfigure;

               p#frm#trac.log_sub_info (
                     'query '
                  || r_query.mesr_query_code
                  || ': SQL executed '
                , 'SQL EXECUTED'
               );

               IF l_keyfigure.FIRST IS NOT NULL THEN
                  FOR i IN l_keyfigure.FIRST .. l_keyfigure.LAST LOOP
                     prc_keyfigure_ins (
                        r_query.mesr_query_code
                      , l_keyfigure (i).keyfigure
                      , l_keyfigure (i).keyfigure
                     );

                     IF p_b_exception_if_fails THEN
                        l_b_success :=
                           fct_exec_verify (
                              r_query.mesr_query_code
                            , l_keyfigure (i).keyfigure
                            , l_keyfigure (i).resultvalue
                           );
                     END IF;

                     prc_exec_ins (
                        r_query.mesr_query_code
                      , l_keyfigure (i).keyfigure
                      , l_keyfigure (i).resultvalue
                      , NULL
                     );
                     p#frm#trac.log_sub_info (
                           'Key figure '
                        || l_keyfigure (i).keyfigure
                        || ' = '
                        || l_keyfigure (i).resultvalue
                        || ' , result stored'
                      , 'KEY FIGURE STORED'
                     );
                  END LOOP;
               ELSE
                  p#frm#trac.log_sub_info (
                        'query '
                     || r_query.mesr_query_code
                     || ': no rows returned '
                   , 'NO RESULTS'
                  );
               END IF;
            ELSIF p_vc_storage_type = 'REPORT' THEN
               l_vc_query_table :=
                     'tmp_mesr_query_'
                  || TRIM (TO_CHAR (
                              r_query.mesr_query_id
                            , '0000000000'
                           ));

               BEGIN
                  l_vc_stmt :=
                        'DROP TABLE '
                     || l_vc_query_table;

                  EXECUTE IMMEDIATE l_vc_stmt;
               EXCEPTION
                  WHEN OTHERS THEN
                     NULL;
               END;

               l_vc_stmt :=
                     'CREATE TABLE '
                  || l_vc_query_table
                  || ' AS '
                  || r_query.mesr_query_sql;

               EXECUTE IMMEDIATE l_vc_stmt;

               p#frm#trac.log_sub_info (
                     'query '
                  || r_query.mesr_query_code
                  || ': Table created '
                , 'SQL EXECUTED'
               );
               l_vc_report :=
                  p#frm#docu.fct_get_table_dataset (
                     USER
                   , l_vc_query_table
                  );
               prc_keyfigure_ins (
                  r_query.mesr_query_code
                , 'REPORT'
                , 'REPORT'
               );
               prc_exec_ins (
                  r_query.mesr_query_code
                , 'REPORT'
                , NULL
                , l_vc_report
               );
               p#frm#trac.log_sub_info (
                  'Report stored'
                , 'REPORT STORED'
               );
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               p#frm#trac.log_error (
                     'query '
                  || r_query.mesr_query_code
                  || ': '
                  || SQLERRM
                , 'ERROR'
               );
         END;

         p#frm#trac.log_sub_info (
               'query '
            || r_query.mesr_query_code
          , 'query FINISH'
         );
      END LOOP;

      p#frm#trac.log_sub_info (
            'Execute query '
         || p_vc_query_code
         || ' : success '
         || CASE
               WHEN l_b_success THEN
                  'TRUE'
               ELSE
                  'FALSE'
            END
       , 'CASE FINISH'
      );

      IF p_b_exception_if_fails
     AND NOT l_b_success THEN
         raise_application_error (
            -20001
          , 'Test failed'
         );
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_error (
               'query '
            || p_vc_query_code
            || ' : failed'
          , 'QUERY ERROR'
         );
         RAISE;
   END;

   PROCEDURE prc_exec_taxonomy (
      p_vc_taxonomy_code       IN VARCHAR2
    , p_b_exception_if_fails   IN BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name   t_object_name := 'PRC_EXEC_TAXONOMY';
   BEGIN
      p#frm#trac.log_sub_info (
            'Executing all cases belonging to taxonomy '
         || p_vc_taxonomy_code
         || ' and its children'
       , l_vc_prc_name
      );

      FOR r_tax IN (    SELECT taxn_id
                             , taxn_name
                             , SYS_CONNECT_BY_PATH (
                                  taxn_code
                                , '/'
                               )
                                  taxn_path
                          FROM p#frm#taxn_t
                    START WITH taxn_code = p_vc_taxonomy_code
                    CONNECT BY PRIOR taxn_id = taxn_parent_id) LOOP
         p#frm#trac.log_sub_info (
               'Executing all cases belonging to taxonomy '
            || r_tax.taxn_path
          , l_vc_prc_name
         );

         FOR r_query IN (SELECT c.mesr_query_code
                           FROM p#frm#mesr_taxn_t t
                              , p#frm#mesr_query_t c
                          WHERE t.mesr_query_id = c.mesr_query_id
                            AND t.taxn_id = r_tax.taxn_id) LOOP
            prc_exec (
               r_query.mesr_query_code
             , p_b_exception_if_fails
             , p_vc_storage_type
            );
         END LOOP;

         p#frm#trac.log_sub_info (
               'All cases belonging to taxonomy '
            || r_tax.taxn_path
            || ' have been executed'
          , l_vc_prc_name
         );
      END LOOP;

      p#frm#trac.log_sub_info (
            'All cases belonging to taxonomy '
         || p_vc_taxonomy_code
         || ' and its children have been executed'
       , l_vc_prc_name
      );
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: mesr-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/mesr/mesr-impl.sql $';
END p#frm#mesr;