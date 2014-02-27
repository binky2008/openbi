CREATE OR REPLACE PACKAGE BODY mes_main
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
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_n_exec_value        IN   NUMBER
   )
      RETURN BOOLEAN
   IS
      l_vc_prc_name         pkg_utl_type.vc_obj_plsql := 'FCT_EXEC_VERIFY';
      l_n_keyfigure_id      NUMBER;
      l_vc_threshold_type   CHAR (1);
      l_n_threshold_min     NUMBER;
      l_n_threshold_max     NUMBER;
      l_n_result_previous   NUMBER;
      l_n_increment         NUMBER;
      l_n_cnt               NUMBER                    := 0;
      l_b_success           BOOLEAN                   := TRUE;
   BEGIN
      SELECT MIN (k.qc_keyfigure_id)
           , MIN (t.qc_threshold_type)
           , MIN (t.qc_threshold_min)
           , MAX (t.qc_threshold_max)
        INTO l_n_keyfigure_id
           , l_vc_threshold_type
           , l_n_threshold_min
           , l_n_threshold_max
        FROM qc_case_t d
           , qc_step_t s
           , qc_keyfigure_t k
           , qc_threshold_t t
       WHERE d.qc_case_id = s.qc_case_id
         AND s.qc_step_id = k.qc_step_id
         AND t.qc_keyfigure_id = k.qc_keyfigure_id
         AND d.qc_case_code = p_vc_case_code
         AND s.qc_step_code = p_vc_step_code
         AND k.qc_keyfigure_code = p_vc_keyfigure_code
         AND t.qc_threshold_from <= SYSDATE
         AND SYSDATE < t.qc_threshold_to;

      pkg_utl_log.LOG ('Key figure ' || p_vc_keyfigure_code || ' type ' || l_vc_threshold_type || ' threshold = ' || l_n_threshold_min || ' - ' || l_n_threshold_max, 'VERIFYING');

      IF l_vc_threshold_type = 'A'
      THEN
         IF     l_n_threshold_min IS NOT NULL
            AND l_n_threshold_max IS NOT NULL
            AND p_n_exec_value NOT BETWEEN l_n_threshold_min AND l_n_threshold_max
         THEN
            l_b_success    := FALSE;
            pkg_utl_log.LOG ('Result ' || p_n_exec_value || ' not ok', 'RESULT NOT OK');
         ELSE
            pkg_utl_log.LOG ('Result ' || p_n_exec_value || ' ok', 'RESULT OK');
         END IF;
      ELSIF l_vc_threshold_type = 'I'
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM qc_exec_t
          WHERE qc_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0
         THEN
            SELECT MAX (NVL (qc_exec_result_value, 0))
              INTO l_n_result_previous
              FROM (SELECT qc_exec_id
                         , qc_exec_result_value
                         , MAX (qc_exec_id) OVER (PARTITION BY qc_keyfigure_id) AS qc_exec_last
                      FROM qc_exec_t
                     WHERE qc_keyfigure_id = l_n_keyfigure_id)
             WHERE qc_exec_id = qc_exec_last;

            pkg_utl_log.LOG ('Previous result = ' || l_n_result_previous, 'VERIFYING INCREMENT');

            IF l_n_result_previous > 0
            THEN
               l_n_increment    := (p_n_exec_value - l_n_result_previous) / l_n_result_previous;

               IF     l_n_threshold_min IS NOT NULL
                  AND l_n_threshold_max IS NOT NULL
                  AND l_n_increment NOT BETWEEN l_n_threshold_min AND l_n_threshold_max
               THEN
                  l_b_success    := FALSE;
                  pkg_utl_log.LOG ('Increment ' || l_n_increment || ' not ok', 'RESULT NOT OK');
               ELSE
                  pkg_utl_log.LOG ('Increment ' || l_n_increment || ' ok', 'RESULT OK');
               END IF;
            ELSE
               pkg_utl_log.LOG ('Previous result = ' || l_n_result_previous, 'RESULT OK');
            END IF;
         ELSE
            pkg_utl_log.LOG ('Key figure ' || p_vc_keyfigure_code || ' type ' || l_vc_threshold_type || ' - no previous results available', 'RESULT OK');
         END IF;
      END IF;

      RETURN l_b_success;
   END fct_exec_verify;

   PROCEDURE prc_case_taxonomy_ins (
      p_vc_case_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_CASE_TAXONOMY_INS';
   BEGIN
      pkg_utl_log.LOG ('Inserting in qc_case_taxonomy_t', l_vc_prc_name);
      MERGE INTO qc_case_taxonomy_t trg
         USING (SELECT qc_case_id
                     , sys_taxonomy_id
                  FROM qc_case_t c
                     , sys_taxonomy_t t
                 WHERE c.qc_case_code = p_vc_case_code
                   AND t.sys_taxonomy_code = p_vc_taxonomy_code) src
         ON (    trg.qc_case_id = src.qc_case_id
             AND trg.sys_taxonomy_id = src.sys_taxonomy_id)
         WHEN NOT MATCHED THEN
            INSERT (trg.qc_case_id, trg.sys_taxonomy_id)
            VALUES (src.qc_case_id, src.sys_taxonomy_id);
      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_case_taxonomy_ins;

   PROCEDURE prc_case_taxonomy_del (
      p_vc_case_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_CASE_TAXONOMY_DEL';
   BEGIN
      pkg_utl_log.LOG ('Deleting in qc_case_taxonomy_t', l_vc_prc_name);

      DELETE      qc_case_taxonomy_t
            WHERE qc_case_id = (SELECT qc_case_id
                                  FROM qc_case_t
                                 WHERE qc_case_code = p_vc_case_code)
              AND sys_taxonomy_id = (SELECT sys_taxonomy_id
                                       FROM sys_taxonomy_t
                                      WHERE sys_taxonomy_code = p_vc_taxonomy_code);

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_case_taxonomy_del;

   PROCEDURE prc_case_ins (
      p_vc_case_code          IN   VARCHAR2
    , p_vc_case_name          IN   VARCHAR2
    , p_vc_layer_code         IN   VARCHAR2 DEFAULT 'GLOBAL'
    , p_vc_entity_code        IN   VARCHAR2 DEFAULT 'GLOBAL'
    , p_vc_environment_code   IN   VARCHAR2 DEFAULT 'GLOBAL'
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_CASE_INS';
   BEGIN
      pkg_utl_log.LOG ('Inserting in qc_case_t', l_vc_prc_name);
      MERGE INTO qc_case_t trg
         USING (SELECT p_vc_case_code AS case_code
                     , p_vc_case_name AS case_name
                     , l.sys_layer_id AS layer_id
                     , e.sys_entity_id AS entity_id
                     , n.sys_environment_id AS environment_id
                  FROM sys_layer_t l
                     , sys_entity_t e
                     , sys_environment_t n
                 WHERE l.sys_layer_code(+) = p_vc_layer_code
                   AND e.sys_entity_code(+) = p_vc_entity_code
                   AND n.sys_environment_code(+) = p_vc_environment_code) src
         ON (trg.qc_case_code = src.case_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.qc_case_name = src.case_name, trg.sys_layer_id = src.layer_id, trg.sys_entity_id = src.entity_id, trg.sys_environment_id = src.environment_id
         WHEN NOT MATCHED THEN
            INSERT (trg.qc_case_code, trg.qc_case_name, trg.sys_layer_id, trg.sys_entity_id, trg.sys_environment_id)
            VALUES (src.case_code, src.case_name, src.layer_id, src.entity_id, src.environment_id);
      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_case_ins;

   PROCEDURE prc_case_del (
      p_vc_case_code   IN   VARCHAR2
    , p_b_cascade      IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_CASE_DEL';
      l_n_case_id     NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      -- Get the step id
      SELECT qc_case_id
        INTO l_n_case_id
        FROM qc_case_t c
       WHERE qc_case_code = p_vc_case_code;

      IF NOT p_b_cascade
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM qc_step_t
          WHERE qc_case_id = l_n_case_id;

         IF l_n_cnt > 0
         THEN
            raise_application_error (-20001, 'Cannot delete case with steps');
         END IF;
      END IF;

      -- Delete case-taxonomy assignments
      DELETE      qc_case_taxonomy_t
            WHERE qc_case_id = (SELECT qc_case_id
                                  FROM qc_case_t
                                 WHERE qc_case_code = p_vc_case_code);

      -- Delete children steps
      FOR r_step IN (SELECT qc_step_code
                       FROM qc_step_t
                      WHERE qc_case_id = l_n_case_id)
      LOOP
         prc_step_del (p_vc_case_code
                     , r_step.qc_step_code
                     , p_b_cascade
                      );
      END LOOP;

      DELETE      qc_case_t
            WHERE qc_case_id = l_n_case_id;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_case_del;

   PROCEDURE prc_step_ins (
      p_vc_case_code   IN   VARCHAR2
    , p_n_step_order   IN   NUMBER
    , p_vc_step_code   IN   VARCHAR2
    , p_vc_step_name   IN   VARCHAR2
    , p_vc_step_sql    IN   CLOB
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_STEP_INS';
   BEGIN
      MERGE INTO qc_step_t trg
         USING (SELECT qc_case_id
                     , p_n_step_order AS step_order
                     , p_vc_step_code AS step_code
                     , p_vc_step_name AS step_name
                     , p_vc_step_sql AS step_sql
                  FROM qc_case_t
                 WHERE qc_case_code = p_vc_case_code) src
         ON (    trg.qc_case_id = src.qc_case_id
             AND trg.qc_step_code = src.step_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.qc_step_order = NVL (src.step_order, trg.qc_step_order), trg.qc_step_name = NVL (src.step_name, trg.qc_step_name), trg.qc_step_sql = NVL (src.step_sql, trg.qc_step_sql)
         WHEN NOT MATCHED THEN
            INSERT (trg.qc_case_id, trg.qc_step_order, trg.qc_step_code, trg.qc_step_name, trg.qc_step_sql)
            VALUES (src.qc_case_id, src.step_order, src.step_code, src.step_name, src.step_sql);
      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_step_ins;

   PROCEDURE prc_step_del (
      p_vc_case_code   IN   VARCHAR2
    , p_vc_step_code   IN   VARCHAR2
    , p_b_cascade      IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_STEP_DEL';
      l_n_step_id     NUMBER;
      l_n_cnt         NUMBER;
   BEGIN
      -- Get the step id
      SELECT s.qc_step_id
        INTO l_n_step_id
        FROM qc_case_t c
           , qc_step_t s
       WHERE s.qc_case_id = c.qc_case_id
         AND c.qc_case_code = p_vc_case_code
         AND s.qc_step_code = p_vc_step_code;

      IF NOT p_b_cascade
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM qc_keyfigure_t
          WHERE qc_step_id = l_n_step_id;

         IF l_n_cnt > 0
         THEN
            raise_application_error (-20001, 'Cannot delete step with key figures');
         END IF;
      END IF;

      FOR r_key IN (SELECT qc_keyfigure_code
                      FROM qc_keyfigure_t
                     WHERE qc_step_id = l_n_step_id)
      LOOP
         prc_keyfigure_del (p_vc_case_code
                          , p_vc_step_code
                          , r_key.qc_keyfigure_code
                          , p_b_cascade
                           );
      END LOOP;

      DELETE      qc_step_t
            WHERE qc_step_id = l_n_step_id;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_step_del;

   PROCEDURE prc_keyfigure_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_keyfigure_name   IN   VARCHAR2
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_KEYFIGURE_INS';
   BEGIN
      MERGE INTO qc_keyfigure_t trg
         USING (SELECT s.qc_step_id
                     , p_vc_keyfigure_code AS keyfigure_code
                     , p_vc_keyfigure_name AS keyfigure_name
                  FROM qc_case_t d
                     , qc_step_t s
                 WHERE s.qc_case_id = d.qc_case_id
                   AND d.qc_case_code = p_vc_case_code
                   AND s.qc_step_code = p_vc_step_code) src
         ON (    trg.qc_step_id = src.qc_step_id
             AND trg.qc_keyfigure_code = src.keyfigure_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.qc_keyfigure_name = NVL (src.keyfigure_name, trg.qc_keyfigure_name)
         WHEN NOT MATCHED THEN
            INSERT (trg.qc_step_id, trg.qc_keyfigure_code, trg.qc_keyfigure_name)
            VALUES (src.qc_step_id, src.keyfigure_code, src.keyfigure_name);
      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows merged', l_vc_prc_name);
      COMMIT;
   END prc_keyfigure_ins;

   PROCEDURE prc_keyfigure_del (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_b_cascade           IN   BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name      pkg_utl_type.vc_obj_plsql := 'PRC_KEYFIGURE_DEL';
      l_n_keyfigure_id   NUMBER;
      l_n_cnt            NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.qc_keyfigure_id
        INTO l_n_keyfigure_id
        FROM qc_case_t c
           , qc_step_t s
           , qc_keyfigure_t k
       WHERE s.qc_case_id = c.qc_case_id
         AND s.qc_step_id = k.qc_step_id
         AND c.qc_case_code = p_vc_case_code
         AND s.qc_step_code = p_vc_step_code
         AND k.qc_keyfigure_code = p_vc_keyfigure_code;

      IF NOT p_b_cascade
      THEN
         SELECT COUNT (*)
           INTO l_n_cnt
           FROM qc_exec_t
          WHERE qc_keyfigure_id = l_n_keyfigure_id;

         IF l_n_cnt > 0
         THEN
            raise_application_error (-20001, 'Cannot delete key figure with execution results');
         END IF;
      END IF;

      DELETE      qc_exec_t
            WHERE qc_keyfigure_id = l_n_keyfigure_id;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

      DELETE      qc_threshold_t
            WHERE qc_keyfigure_id = l_n_keyfigure_id;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

      DELETE      qc_keyfigure_t
            WHERE qc_keyfigure_id = l_n_keyfigure_id;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);
      COMMIT;
   END prc_keyfigure_del;

   PROCEDURE prc_threshold_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_vc_threshold_type   IN   VARCHAR2
    , p_n_threshold_min     IN   NUMBER
    , p_n_threshold_max     IN   NUMBER
    , p_d_threshold_from    IN   DATE DEFAULT TO_DATE ('01011111', 'ddmmyyyy')
    , p_d_threshold_to      IN   DATE DEFAULT TO_DATE ('09099999', 'ddmmyyyy')
   )
   IS
      l_vc_prc_name        pkg_utl_type.vc_obj_plsql := 'PRC_THRESHOLD_INS';
      l_d_threshold_from   DATE                      := NVL (p_d_threshold_from, TO_DATE ('01011111', 'ddmmyyyy'));
      l_d_threshold_to     DATE                      := NVL (p_d_threshold_to, TO_DATE ('09099999', 'ddmmyyyy'));
      l_n_keyfigure_id     NUMBER;
      l_n_threshold_id     NUMBER;
      l_n_split_flag       NUMBER;
      l_n_split_min        NUMBER;
      l_n_split_max        NUMBER;
   BEGIN
      -- Get the key figure id
      SELECT k.qc_keyfigure_id
        INTO l_n_keyfigure_id
        FROM qc_case_t c
           , qc_step_t s
           , qc_keyfigure_t k
       WHERE s.qc_case_id = c.qc_case_id
         AND s.qc_step_id = k.qc_step_id
         AND c.qc_case_code = p_vc_case_code
         AND s.qc_step_code = p_vc_step_code
         AND k.qc_keyfigure_code = p_vc_keyfigure_code;

      IF l_n_keyfigure_id IS NOT NULL
      THEN
         -- Delete existing time slices if they reside between new boundary
         DELETE      qc_threshold_t
               WHERE qc_keyfigure_id = l_n_keyfigure_id
                 AND qc_threshold_from > l_d_threshold_from
                 AND qc_threshold_to < l_d_threshold_to;

         pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows deleted', l_vc_prc_name);

         -- If new slice inside existing then split
         INSERT INTO qc_threshold_t
                     (qc_keyfigure_id
                    , qc_threshold_type
                    , qc_threshold_min
                    , qc_threshold_max
                    , qc_threshold_from
                    , qc_threshold_to
                     )
            SELECT qc_keyfigure_id
                 , qc_threshold_type
                 , qc_threshold_min
                 , qc_threshold_max
                 , l_d_threshold_to
                 , qc_threshold_to
              FROM qc_threshold_t
             WHERE qc_keyfigure_id = l_n_keyfigure_id
               AND qc_threshold_from < l_d_threshold_from
               AND qc_threshold_to > l_d_threshold_to;

         pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);

         -- Update existing time slice where upper bound > new lower bound
         UPDATE qc_threshold_t
            SET qc_threshold_to = l_d_threshold_from
          WHERE qc_keyfigure_id = l_n_keyfigure_id
            AND qc_threshold_from < l_d_threshold_from
            AND qc_threshold_to > l_d_threshold_from;

         pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         -- Update existing time slice where lower bound < new upper bound
         UPDATE qc_threshold_t
            SET qc_threshold_from = l_d_threshold_to
          WHERE qc_keyfigure_id = l_n_keyfigure_id
            AND qc_threshold_to > l_d_threshold_to
            AND qc_threshold_from < l_d_threshold_to;

         pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         -- Update time slice with same boundary
         UPDATE    qc_threshold_t
               SET qc_threshold_type = p_vc_threshold_type
                 , qc_threshold_min = p_n_threshold_min
                 , qc_threshold_max = p_n_threshold_max
             WHERE qc_keyfigure_id = l_n_keyfigure_id
               AND qc_threshold_from = l_d_threshold_from
               AND qc_threshold_to = l_d_threshold_to
         RETURNING qc_threshold_id
              INTO l_n_threshold_id;

         pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows updated', l_vc_prc_name);

         IF l_n_threshold_id IS NULL
         THEN
            INSERT INTO qc_threshold_t
                        (qc_keyfigure_id
                       , qc_threshold_type
                       , qc_threshold_min
                       , qc_threshold_max
                       , qc_threshold_from
                       , qc_threshold_to
                        )
                 VALUES (l_n_keyfigure_id
                       , p_vc_threshold_type
                       , p_n_threshold_min
                       , p_n_threshold_max
                       , l_d_threshold_from
                       , l_d_threshold_to
                        );

            pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);
         END IF;

         COMMIT;
      END IF;
   END prc_threshold_ins;

   PROCEDURE prc_exec_ins (
      p_vc_case_code        IN   VARCHAR2
    , p_vc_step_code        IN   VARCHAR2
    , p_vc_keyfigure_code   IN   VARCHAR2
    , p_n_result_value      IN   NUMBER
    , p_vc_result_report    IN   CLOB
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_EXEC_INS';
   BEGIN
      INSERT INTO qc_exec_t
                  (qc_keyfigure_id
                 , qc_exec_result_value
                 , qc_exec_result_report
                  )
         SELECT k.qc_keyfigure_id
              , p_n_result_value
              , p_vc_result_report
           FROM qc_case_t d
              , qc_step_t s
              , qc_keyfigure_t k
          WHERE d.qc_case_id = s.qc_case_id
            AND s.qc_step_id = k.qc_step_id
            AND d.qc_case_code = p_vc_case_code
            AND s.qc_step_code = p_vc_step_code
            AND k.qc_keyfigure_code = p_vc_keyfigure_code;

      pkg_utl_log.LOG (SQL%ROWCOUNT || ' rows inserted', l_vc_prc_name);
      COMMIT;
   END prc_exec_ins;

   PROCEDURE prc_exec (
      p_vc_case_code           IN   VARCHAR2 DEFAULT 'ALL'
    , p_vc_step_code           IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name       pkg_utl_type.vc_obj_plsql := 'PRC_EXEC';
      l_keyfigure         t_keyvalue;
      l_vc_step_table     VARCHAR2 (100);
      l_vc_stmt           VARCHAR2 (32000);
      l_vc_report         CLOB;
      l_vc_job_name       VARCHAR2 (100);
      l_n_gui             NUMBER;
      l_n_step_no         NUMBER;
      l_n_result          NUMBER;
      l_n_threshold_min   NUMBER;
      l_n_threshold_max   NUMBER;
      l_b_success         BOOLEAN                   := TRUE;
   BEGIN
      l_vc_job_name    := 'QC_' || p_vc_case_code;
      pkg_utl_log.set_workflow_name ('QC_EXEC');
      l_n_result       := pkg_utl_job.initialize (l_vc_job_name
                                                , 'QC'
                                                , l_n_gui
                                                , l_n_step_no
                                                 );
      l_n_result       := pkg_utl_job.set_step_no (l_vc_job_name
                                                 , 'QC'
                                                 , l_n_gui
                                                 , 0
                                                 , 'BEGIN'
                                                  );
      pkg_utl_log.set_console_logging (FALSE);
      pkg_utl_log.LOG ('Execute case ' || p_vc_case_code || ', steps ' || p_vc_step_code, 'CASE START');
      pkg_utl_log.LOG ('Results will be stored as ' || p_vc_storage_type, 'STORAGE ' || p_vc_storage_type);

      FOR r_step IN (SELECT   d.qc_case_code
                            , s.qc_step_id
                            , s.qc_step_code
                            , s.qc_step_sql
                         FROM qc_case_t d
                            , qc_step_t s
                        WHERE d.qc_case_id = s.qc_case_id
                          AND (   p_vc_case_code IN (d.qc_case_code, 'ALL')
                               OR p_vc_case_code IS NULL)
                          AND (   p_vc_step_code IN (s.qc_step_code, 'ALL')
                               OR p_vc_step_code IS NULL)
                     ORDER BY d.qc_case_code
                            , s.qc_step_order
                            , s.qc_step_code)
      LOOP
         pkg_utl_log.LOG ('Step ' || r_step.qc_step_code, 'STEP START');

         BEGIN
            IF    p_vc_storage_type = 'VALUE'
               OR p_vc_storage_type IS NULL
            THEN
               EXECUTE IMMEDIATE r_step.qc_step_sql
               BULK COLLECT INTO l_keyfigure;

               pkg_utl_log.LOG ('Step ' || r_step.qc_step_code || ': SQL executed ', 'SQL EXECUTED');

               IF l_keyfigure.FIRST IS NOT NULL
               THEN
                  FOR i IN l_keyfigure.FIRST .. l_keyfigure.LAST
                  LOOP
                     pkg_qc.prc_keyfigure_ins (r_step.qc_case_code
                                             , r_step.qc_step_code
                                             , l_keyfigure (i).keyfigure
                                             , l_keyfigure (i).keyfigure
                                              );

                     IF p_b_exception_if_fails
                     THEN
                        l_b_success    := fct_exec_verify (r_step.qc_case_code
                                                         , r_step.qc_step_code
                                                         , l_keyfigure (i).keyfigure
                                                         , l_keyfigure (i).resultvalue
                                                          );
                     END IF;

                     pkg_qc.prc_exec_ins (r_step.qc_case_code
                                        , r_step.qc_step_code
                                        , l_keyfigure (i).keyfigure
                                        , l_keyfigure (i).resultvalue
                                        , NULL
                                         );
                     pkg_utl_log.LOG ('Key figure ' || l_keyfigure (i).keyfigure || ' = ' || l_keyfigure (i).resultvalue || ' , result stored', 'KEY FIGURE STORED');
                  END LOOP;
               ELSE
                  pkg_utl_log.LOG ('Step ' || r_step.qc_step_code || ': no rows returned ', 'NO RESULTS');
               END IF;
            ELSIF p_vc_storage_type = 'REPORT'
            THEN
               l_vc_step_table    := 'tmp_qc_step_' || TRIM (TO_CHAR (r_step.qc_step_id, '0000000000'));

               BEGIN
                  l_vc_stmt    := 'DROP TABLE ' || l_vc_step_table;

                  EXECUTE IMMEDIATE l_vc_stmt;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     NULL;
               END;

               l_vc_stmt          := 'CREATE TABLE ' || l_vc_step_table || ' AS ' || r_step.qc_step_sql;

               EXECUTE IMMEDIATE l_vc_stmt;

               pkg_utl_log.LOG ('Step ' || r_step.qc_step_code || ': Table created ', 'SQL EXECUTED');
               l_vc_report        := pkg_utl_doc.fct_get_table_dataset (SYS_CONTEXT ('USERENV', 'SESSION_USER'), l_vc_step_table);
               pkg_qc.prc_keyfigure_ins (r_step.qc_case_code
                                       , r_step.qc_step_code
                                       , 'REPORT'
                                       , 'REPORT'
                                        );
               pkg_qc.prc_exec_ins (r_step.qc_case_code
                                  , r_step.qc_step_code
                                  , 'REPORT'
                                  , NULL
                                  , l_vc_report
                                   );
               pkg_utl_log.LOG ('Report stored', 'REPORT STORED');
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               pkg_utl_log.LOG ('Step ' || r_step.qc_step_code || ': ' || SQLERRM
                              , 'ERROR'
                              , pkg_utl_log.gc_error
                              , SQLCODE
                               );
         END;

         pkg_utl_log.LOG ('Step ' || r_step.qc_step_code, 'STEP FINISH');
      END LOOP;

      pkg_utl_log.LOG ('Execute case ' || p_vc_case_code || ', steps ' || p_vc_step_code || ' : success ' || CASE
                          WHEN l_b_success
                             THEN 'TRUE'
                          ELSE 'FALSE'
                       END, 'CASE FINISH');
      l_n_result       := pkg_utl_job.set_step_no (l_vc_job_name
                                                 , 'QC'
                                                 , l_n_gui
                                                 , 1
                                                 , 'END'
                                                  );
      l_n_result       := pkg_utl_job.finalize (l_vc_job_name
                                              , 'QC'
                                              , l_n_gui
                                               );

      IF     p_b_exception_if_fails
         AND NOT l_b_success
      THEN
         raise_application_error (-20001, 'Test failed');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         pkg_utl_log.LOG ('Step ' || p_vc_case_code || ', steps ' || p_vc_step_code || ' : failed'
                        , 'CASE ERROR'
                        , pkg_utl_log.gc_error
                        , SQLCODE
                         );
         l_n_result    := pkg_utl_job.set_error_status (l_vc_job_name
                                                      , 'QC'
                                                      , l_n_gui
                                                      , SQLERRM
                                                       );
         RAISE;
   END;

   PROCEDURE prc_exec_taxonomy (
      p_vc_taxonomy_code       IN   VARCHAR2
    , p_b_exception_if_fails   IN   BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN   VARCHAR2 DEFAULT 'VALUE'
   )
   IS
      l_vc_prc_name   pkg_utl_type.vc_obj_plsql := 'PRC_EXEC_TAXONOMY';
   BEGIN
      pkg_utl_log.LOG ('Executing all cases belonging to taxonomy ' || p_vc_taxonomy_code || ' and its children', l_vc_prc_name);

      FOR r_tax IN (SELECT     sys_taxonomy_id
                             , sys_taxonomy_name
                             , SYS_CONNECT_BY_PATH (sys_taxonomy_code, '/') sys_taxonomy_path
                          FROM sys_taxonomy_t
                    START WITH sys_taxonomy_code = p_vc_taxonomy_code
                    CONNECT BY PRIOR sys_taxonomy_id = sys_taxonomy_parent_id)
      LOOP
         pkg_utl_log.LOG ('Executing all cases belonging to taxonomy ' || r_tax.sys_taxonomy_path, l_vc_prc_name);

         FOR r_case IN (SELECT c.qc_case_code
                          FROM qc_case_taxonomy_t t
                             , qc_case_t c
                         WHERE t.qc_case_id = c.qc_case_id
                           AND t.sys_taxonomy_id = r_tax.sys_taxonomy_id)
         LOOP
            prc_exec (r_case.qc_case_code
                    , 'ALL'
                    , p_b_exception_if_fails
                    , p_vc_storage_type
                     );
         END LOOP;

         pkg_utl_log.LOG ('All cases belonging to taxonomy ' || r_tax.sys_taxonomy_path || ' have been executed', l_vc_prc_name);
      END LOOP;

      pkg_utl_log.LOG ('All cases belonging to taxonomy ' || p_vc_taxonomy_code || ' and its children have been executed', l_vc_prc_name);
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_qc-impl.sql 2288 2012-02-02 15:24:13Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_qc/pkg_qc-impl.sql $';
END mes_main;
/

SHOW errors