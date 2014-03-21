CREATE OR REPLACE PACKAGE BODY mes
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-02-02 16:24:13 +0100 (Do, 02 Feb 2012) $
    * $Revision: 2288 $
    * $Id: pkg_qc-impl.sql 2288 2012-02-02 15:24:13Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_qc/pkg_qc-impl.sql $
    */
   TYPE r_keyvalue IS RECORD (
      keyfigure     VARCHAR2 (100)
    , resultvalue   NUMBER
   );

   TYPE t_keyvalue IS TABLE OF r_keyvalue;

   FUNCTION fct_exec_verify (
      p_vc_query_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_n_exec_value        IN   NUMBER
   )
      RETURN BOOLEAN
   IS
      l_vc_prc_name         type.vc_obj_plsql := 'FCT_EXEC_VERIFY';
      l_n_keyfigure_id      NUMBER;
      l_vc_threshold_type   CHAR (1);
      l_n_threshold_min     NUMBER;
      l_n_threshold_max     NUMBER;
      l_n_result_previous   NUMBER;
      l_n_increment         NUMBER;
      l_n_cnt               NUMBER                    := 0;
      l_b_success           BOOLEAN                   := TRUE;
   BEGIN
      SELECT MIN (k.mes_keyfigure_id)
           , MIN (t.mes_threshold_type)
           , MIN (t.mes_threshold_min)
           , MAX (t.mes_threshold_max)
        INTO l_n_keyfigure_id
           , l_vc_threshold_type
           , l_n_threshold_min
           , l_n_threshold_max
        FROM mes_query_t s
           , mes_keyfigure_t k
           , mes_threshold_t t
       WHERE s.mes_query_id = k.mes_query_id
         AND t.mes_keyfigure_id = k.mes_keyfigure_id
         AND s.mes_query_code = p_vc_query_code
         AND k.mes_keyfigure_code = p_vc_keyfigure_code
         AND t.mes_threshold_from <= SYSDATE
         AND SYSDATE < t.mes_threshold_to;

       trc.log_info_SUB_INFO ('Key figure ' || p_vc_keyfigure_code || ' type ' || l_vc_threshold_type || ' threshold = ' || l_n_threshold_min || ' - ' || l_n_threshold_max, 'VERIFYING');

      IF l_vc_threshold_type = 'A'
      THEN
         IF     l_n_threshold_min IS NOT NULL
            AND l_n_threshold_max IS NOT NULL
            AND p_n_exec_value NOT BETWEEN l_n_threshold_min AND l_n_threshold_max
         THEN
            l_b_success    := FALSE;
             trc.log_info_SUB_INFO ('Result ' || p_n_exec_value || ' not ok', 'RESULT NOT OK');
         ELSE
             trc.log_info_SUB_INFO ('Result ' || p_n_exec_value || ' ok', 'RESULT OK');
         END IF;
      ELSIF l_vc_threshold_type = 'I'
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM mes_exec_t
          WHERE mes_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0
         THEN
            SELECT MAX (NVL (mes_exec_result_value, 0))
              INTO l_n_result_previous
              FROM (SELECT mes_exec_id
                         , mes_exec_result_value
                         , MAX (mes_exec_id) OVER (PARTITION BY mes_keyfigure_id) AS mes_exec_last
                      FROM mes_exec_t
                     WHERE mes_keyfigure_id = l_n_keyfigure_id)
             WHERE mes_exec_id = mes_exec_last;

             trc.log_info_SUB_INFO ('Previous result = ' || l_n_result_previous, 'VERIFYING INCREMENT');

            IF l_n_result_previous > 0
            THEN
               l_n_increment    := (p_n_exec_value - l_n_result_previous) / l_n_result_previous;

               IF     l_n_threshold_min IS NOT NULL
                  AND l_n_threshold_max IS NOT NULL
                  AND l_n_increment NOT BETWEEN l_n_threshold_min AND l_n_threshold_max
               THEN
                  l_b_success    := FALSE;
                   trc.log_info_SUB_INFO ('Increment ' || l_n_increment || ' not ok', 'RESULT NOT OK');
               ELSE
                   trc.log_info_SUB_INFO ('Increment ' || l_n_increment || ' ok', 'RESULT OK');
               END IF;
            ELSE
                trc.log_info_SUB_INFO ('Previous result = ' || l_n_result_previous, 'RESULT OK');
            END IF;
         ELSE
             trc.log_info_SUB_INFO ('Key figure ' || p_vc_keyfigure_code || ' type ' || l_vc_threshold_type || ' - no previous results available', 'RESULT OK');
         END IF;
      END IF;

      RETURN l_b_success;
   END fct_exec_verify;

   PROCEDURE prc_mes_txn_ins (
      p_vc_query_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_MES_TAXONOMY_INS';
   BEGIN
       trc.log_info_SUB_INFO (l_vc_prc_name,'Inserting in mes_case_taxonomy_t');
      MERGE INTO mes_txn_t trg
         USING (SELECT mes_query_id
                     , txn_id
                  FROM mes_query_t c
                     , txn_t t
                 WHERE c.mes_query_code = p_vc_query_code
                   AND t.txn_code = p_vc_taxonomy_code) src
         ON (    trg.mes_query_id = src.mes_query_id
             AND trg.txn_id = src.txn_id)
         WHEN NOT MATCHED THEN
            INSERT (trg.mes_query_id, trg.txn_id)
            VALUES (src.mes_query_id, src.txn_id);
       trc.log_info_SUB_INFO (l_vc_prc_name, SQL%ROWCOUNT || ' rows merged');
      COMMIT;
   END prc_mes_txn_ins;

   PROCEDURE prc_mes_txn_del (
      p_vc_query_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_CASE_TAXONOMY_DEL';
   BEGIN
       trc.log_info_SUB_INFO ('Deleting in mes_case_taxonomy_t', l_vc_prc_name);

      DELETE      mes_txn_t
            WHERE mes_query_id = (SELECT mes_query_id
                                  FROM mes_query_t
                                 WHERE mes_query_code = p_vc_query_code)
              AND txn_id = (SELECT txn_id
                                       FROM txn_t
                                      WHERE txn_code = p_vc_taxonomy_code);

       trc.log_info_SUB_INFO (l_vc_prc_name, SQL%ROWCOUNT || ' rows deleted');
      COMMIT;
   END prc_mes_txn_del;

   PROCEDURE prc_query_ins (
      p_vc_query_code   IN   VARCHAR2
    , p_vc_query_name   IN   VARCHAR2
    , p_vc_query_sql    IN   CLOB
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_query_INS';
   BEGIN
      MERGE INTO mes_query_t trg
         USING (SELECT p_vc_query_code AS query_code
                     , p_vc_query_name AS query_name
                     , p_vc_query_sql AS query_sql
                  FROM dual) src
         ON (   trg.mes_query_code = src.query_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.mes_query_name = NVL (src.query_name, trg.mes_query_name), trg.mes_query_sql = NVL (src.query_sql, trg.mes_query_sql)
         WHEN NOT MATCHED THEN
            INSERT (trg.mes_query_code, trg.mes_query_name, trg.mes_query_sql)
            VALUES (src.query_code, src.query_name, src.query_sql);
       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_query_ins;

   PROCEDURE prc_query_del (
      p_vc_query_code   IN   VARCHAR2
    , p_b_cascade      IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_query_DEL';
      l_n_query_id     NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      -- Get the query id
      SELECT mes_query_id
        INTO l_n_query_id
        FROM mes_query_t
       WHERE mes_query_code = p_vc_query_code;

      IF NOT p_b_cascade
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM mes_keyfigure_t
          WHERE mes_query_id = l_n_query_id;

         IF l_n_cnt > 0
         THEN
            raise_application_error (-20001, 'Cannot delete query with key figures');
         END IF;
      END IF;

      FOR r_key IN (SELECT mes_keyfigure_code
                      FROM mes_keyfigure_t
                     WHERE mes_query_id = l_n_query_id)
      LOOP
         prc_keyfigure_del (p_vc_query_code
                          , r_key.mes_keyfigure_code
                          , p_b_cascade
                           );
      END LOOP;

      DELETE      mes_query_t
            WHERE mes_query_id = l_n_query_id;

       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_query_del;

   PROCEDURE prc_keyfigure_ins (
      p_vc_query_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_keyfigure_name   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_KEYFIGURE_INS';
   BEGIN
      MERGE INTO mes_keyfigure_t trg
         USING (SELECT s.mes_query_id
                     , p_vc_keyfigure_code AS keyfigure_code
                     , p_vc_keyfigure_name AS keyfigure_name
                  FROM mes_query_t s
                 WHERE s.mes_query_code = p_vc_query_code) src
         ON (    trg.mes_query_id = src.mes_query_id
             AND trg.mes_keyfigure_code = src.keyfigure_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.mes_keyfigure_name = NVL (src.keyfigure_name, trg.mes_keyfigure_name)
         WHEN NOT MATCHED THEN
            INSERT (trg.mes_query_id, trg.mes_keyfigure_code, trg.mes_keyfigure_name)
            VALUES (src.mes_query_id, src.keyfigure_code, src.keyfigure_name);
       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_keyfigure_ins;

   PROCEDURE prc_keyfigure_del (
      p_vc_query_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_b_cascade           IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name      type.vc_obj_plsql := 'PRC_KEYFIGURE_DEL';
      l_n_keyfigure_id   NUMBER;
      l_n_cnt            NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.mes_keyfigure_id
        INTO l_n_keyfigure_id
        FROM mes_query_t s
           , mes_keyfigure_t k
       WHERE s.mes_query_id = k.mes_query_id
         AND s.mes_query_code = p_vc_query_code
         AND k.mes_keyfigure_code = p_vc_keyfigure_code;

      IF NOT p_b_cascade
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM mes_exec_t
          WHERE mes_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0
         THEN
            raise_application_error (-20001, 'Cannot delete key figure with execution results');
         END IF;
      END IF;

      DELETE      mes_exec_t
            WHERE mes_keyfigure_id = l_n_keyfigure_id;

       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

      DELETE      mes_threshold_t
            WHERE mes_keyfigure_id = l_n_keyfigure_id;

       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

      DELETE      mes_keyfigure_t
            WHERE mes_keyfigure_id = l_n_keyfigure_id;

       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_keyfigure_del;

   PROCEDURE prc_threshold_ins (
      p_vc_query_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_threshold_type   IN   VARCHAR2
    , p_n_threshold_min     IN   NUMBER
    , p_n_threshold_max     IN   NUMBER
    , p_d_threshold_from    IN   DATE DEFAULT TO_DATE ('01011111', 'ddmmyyyy')
    , p_d_threshold_to      IN   DATE DEFAULT TO_DATE ('09099999', 'ddmmyyyy')
   )
   IS
      l_vc_prc_name        type.vc_obj_plsql := 'PRC_THRESHOLD_INS';
      l_d_threshold_from   DATE                      := NVL (p_d_threshold_from, TO_DATE ('01011111', 'ddmmyyyy'));
      l_d_threshold_to     DATE                      := NVL (p_d_threshold_to, TO_DATE ('09099999', 'ddmmyyyy'));
      l_n_keyfigure_id     NUMBER;
      l_n_threshold_id     NUMBER;
      l_n_split_flag       NUMBER;
      l_n_split_min        NUMBER;
      l_n_split_max        NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.mes_keyfigure_id
        INTO l_n_keyfigure_id
        FROM mes_query_t s
           , mes_keyfigure_t k
       WHERE s.mes_query_id = k.mes_query_id
         AND s.mes_query_code = p_vc_query_code
         AND k.mes_keyfigure_code = p_vc_keyfigure_code;

      IF l_n_keyfigure_id IS NOT NULL
      THEN
         -- Delete existing time slices if they reside between new boundary
         DELETE      mes_threshold_t
               WHERE mes_keyfigure_id = l_n_keyfigure_id
                 AND mes_threshold_from > l_d_threshold_from
                 AND mes_threshold_to < l_d_threshold_to;

          trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

         -- If new slice inside existing then split
         INSERT INTO mes_threshold_t
                     (mes_keyfigure_id
                    , mes_threshold_type
                    , mes_threshold_min
                    , mes_threshold_max
                    , mes_threshold_from
                    , mes_threshold_to
                     )
            SELECT mes_keyfigure_id
                 , mes_threshold_type
                 , mes_threshold_min
                 , mes_threshold_max
                 , l_d_threshold_to
                 , mes_threshold_to
              FROM mes_threshold_t
             WHERE mes_keyfigure_id = l_n_keyfigure_id
               AND mes_threshold_from < l_d_threshold_from
               AND mes_threshold_to > l_d_threshold_to;

          trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);

         -- Update existing time slice where upper bound > new lower bound
         UPDATE mes_threshold_t
            SET mes_threshold_to = l_d_threshold_from
          WHERE mes_keyfigure_id = l_n_keyfigure_id
            AND mes_threshold_from < l_d_threshold_from
            AND mes_threshold_to > l_d_threshold_from;

          trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         -- Update existing time slice where lower bound < new upper bound
         UPDATE mes_threshold_t
            SET mes_threshold_from = l_d_threshold_to
          WHERE mes_keyfigure_id = l_n_keyfigure_id
            AND mes_threshold_to > l_d_threshold_to
            AND mes_threshold_from < l_d_threshold_to;

          trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         -- Update time slice with same boundary
         UPDATE    mes_threshold_t
               SET mes_threshold_type = p_vc_threshold_type
                 , mes_threshold_min = p_n_threshold_min
                 , mes_threshold_max = p_n_threshold_max
             WHERE mes_keyfigure_id = l_n_keyfigure_id
               AND mes_threshold_from = l_d_threshold_from
               AND mes_threshold_to = l_d_threshold_to
         RETURNING mes_threshold_id
              INTO l_n_threshold_id;

          trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         IF l_n_threshold_id IS NULL
         THEN
            INSERT INTO mes_threshold_t
                        (mes_keyfigure_id
                       , mes_threshold_type
                       , mes_threshold_min
                       , mes_threshold_max
                       , mes_threshold_from
                       , mes_threshold_to
                        )
                 VALUES (l_n_keyfigure_id
                       , p_vc_threshold_type
                       , p_n_threshold_min
                       , p_n_threshold_max
                       , l_d_threshold_from
                       , l_d_threshold_to
                        );

             trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);
         END IF;

         COMMIT;
      END IF;
   END prc_threshold_ins;

   PROCEDURE prc_exec_ins (
      p_vc_query_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_n_result_value      IN   NUMBER
    , p_vc_result_report    IN   CLOB
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_EXEC_INS';
   BEGIN
      INSERT INTO mes_exec_t
                  (mes_keyfigure_id
                 , mes_exec_result_value
                 , mes_exec_result_report
                  )
         SELECT k.mes_keyfigure_id
              , p_n_result_value
              , p_vc_result_report
           FROM mes_query_t s
              , mes_keyfigure_t k
          WHERE s.mes_query_id = k.mes_query_id
            AND s.mes_query_code = p_vc_query_code
            AND k.mes_keyfigure_code = p_vc_keyfigure_code;

       trc.log_info_SUB_INFO (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);
      COMMIT;
   END prc_exec_ins;

   PROCEDURE prc_exec (
      p_vc_query_code           IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name       type.vc_obj_plsql := 'PRC_EXEC';
      l_keyfigure         t_keyvalue;
      l_vc_query_table     VARCHAR2 (100);
      l_vc_stmt           VARCHAR2 (32000);
      l_vc_report         CLOB;
      l_vc_job_name       VARCHAR2 (100);
      l_n_gui             NUMBER;
      l_n_query_no         NUMBER;
      l_n_result          NUMBER;
      l_n_threshold_min   NUMBER;
      l_n_threshold_max   NUMBER;
      l_b_success         BOOLEAN                   := TRUE;
   BEGIN
       trc.log_info_SUB_INFO ('Execute case query ' || p_vc_query_code, 'Query START');
       trc.log_info_SUB_INFO ('Results will be stored as ' || p_vc_storage_type, 'STORAGE ' || p_vc_storage_type);

      FOR r_query IN (SELECT  s.mes_query_id
                            , s.mes_query_code
                            , s.mes_query_sql
                         FROM mes_query_t s
                        WHERE (   p_vc_query_code IN (s.mes_query_code, 'ALL')
                               OR p_vc_query_code IS NULL)
                     ORDER BY s.mes_query_code)
      LOOP
          trc.log_info_SUB_INFO ('query ' || r_query.mes_query_code, 'query START');

         BEGIN
            IF    p_vc_storage_type = 'VALUE'
               OR p_vc_storage_type IS NULL
            THEN
               EXECUTE IMMEDIATE r_query.mes_query_sql
               BULK COLLECT INTO l_keyfigure;

                trc.log_info_SUB_INFO ('query ' || r_query.mes_query_code || ': SQL executed ', 'SQL EXECUTED');

               IF l_keyfigure.FIRST IS NOT NULL
               THEN
                  FOR i IN l_keyfigure.FIRST .. l_keyfigure.LAST
                  LOOP
                     prc_keyfigure_ins (r_query.mes_query_code
                                             , l_keyfigure (i).keyfigure
                                             , l_keyfigure (i).keyfigure
                                              );

                     IF p_b_exception_if_fails
                     THEN
                        l_b_success    := fct_exec_verify (r_query.mes_query_code
                                                         , l_keyfigure (i).keyfigure
                                                         , l_keyfigure (i).resultvalue
                                                          );
                     END IF;

                     prc_exec_ins (r_query.mes_query_code
                                        , l_keyfigure (i).keyfigure
                                        , l_keyfigure (i).resultvalue
                                        , NULL
                                         );
                      trc.log_info_SUB_INFO ('Key figure ' || l_keyfigure (i).keyfigure || ' = ' || l_keyfigure (i).resultvalue || ' , result stored', 'KEY FIGURE STORED');
                  END LOOP;
               ELSE
                   trc.log_info_SUB_INFO ('query ' || r_query.mes_query_code || ': no rows returned ', 'NO RESULTS');
               END IF;
            ELSIF p_vc_storage_type = 'REPORT'
            THEN
               l_vc_query_table    := 'tmp_mes_query_' || TRIM (TO_CHAR (r_query.mes_query_id, '0000000000'));

               BEGIN
                  l_vc_stmt    := 'DROP TABLE ' || l_vc_query_table;

                  EXECUTE IMMEDIATE l_vc_stmt;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     NULL;
               END;

               l_vc_stmt          := 'CREATE TABLE ' || l_vc_query_table || ' AS ' || r_query.mes_query_sql;

               EXECUTE IMMEDIATE l_vc_stmt;

                trc.log_info_SUB_INFO ('query ' || r_query.mes_query_code || ': Table created ', 'SQL EXECUTED');
               l_vc_report        := doc.fct_get_table_dataset (SYS_CONTEXT ('USERENV', 'SESSION_USER'), l_vc_query_table);
               prc_keyfigure_ins (r_query.mes_query_code
                                       , 'REPORT'
                                       , 'REPORT'
                                        );
               prc_exec_ins (r_query.mes_query_code
                                  , 'REPORT'
                                  , NULL
                                  , l_vc_report
                                   );
                trc.log_info_SUB_INFO ('Report stored', 'REPORT STORED');
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
                 trc.log_error ('query ' || r_query.mes_query_code || ': ' || SQLERRM
                              , 'ERROR'
                               );
         END;

          trc.log_info_SUB_INFO ('query ' || r_query.mes_query_code, 'query FINISH');
      END LOOP;

       trc.log_info_SUB_INFO ('Execute query ' || p_vc_query_code || ' : success ' || CASE
                          WHEN l_b_success
                             THEN 'TRUE'
                          ELSE 'FALSE'
                       END, 'CASE FINISH');

      IF     p_b_exception_if_fails
         AND NOT l_b_success
      THEN
         raise_application_error (-20001, 'Test failed');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
           trc.log_error ('query ' || p_vc_query_code || ' : failed'
                        , 'QUERY ERROR'
                         );

         RAISE;
   END;

   PROCEDURE prc_exec_taxonomy (
      p_vc_taxonomy_code       IN   VARCHAR2
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name   type.vc_obj_plsql := 'PRC_EXEC_TAXONOMY';
   BEGIN
       trc.log_info_SUB_INFO ('Executing all cases belonging to taxonomy ' || p_vc_taxonomy_code || ' and its children', l_vc_prc_name);

      FOR r_tax IN (SELECT     txn_id
                             , txn_name
                             , SYS_CONNECT_BY_PATH (txn_code, '/') txn_path
                          FROM txn_t
                    START WITH txn_code = p_vc_taxonomy_code
                    CONNECT BY PRIOR txn_id = txn_parent_id)
      LOOP
          trc.log_info_SUB_INFO ('Executing all cases belonging to taxonomy ' || r_tax.txn_path, l_vc_prc_name);

         FOR r_query IN (SELECT c.mes_query_code
                          FROM mes_txn_t t
                             , mes_query_t c
                         WHERE t.mes_query_id = c.mes_query_id
                           AND t.txn_id = r_tax.txn_id)
         LOOP
            prc_exec (r_query.mes_query_code
                    , p_b_exception_if_fails
                    , p_vc_storage_type
                     );
         END LOOP;

          trc.log_info_SUB_INFO ('All cases belonging to taxonomy ' || r_tax.txn_path || ' have been executed', l_vc_prc_name);
      END LOOP;

       trc.log_info_SUB_INFO ('All cases belonging to taxonomy ' || p_vc_taxonomy_code || ' and its children have been executed', l_vc_prc_name);
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_qc-impl.sql 2288 2012-02-02 15:24:13Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_qc/pkg_qc-impl.sql $';
END mes;
/

SHOW errors