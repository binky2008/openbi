CREATE OR REPLACE PACKAGE BODY stg_ctl
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-06-20 15:27:31 +0200 (Mi, 20 Jun 2012) $
    * $Revision: 2876 $
    * $Id: stg_ctl-impl.sql 2876 2012-06-20 13:27:31Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ctl/stg_ctl-impl.sql $
    */
   PROCEDURE prc_queue_ins (
      p_vc_queue_code   VARCHAR2
    , p_vc_queue_name   VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO stg_queue_t trg
         USING (SELECT p_vc_queue_code AS queue_code
                     , p_vc_queue_name AS queue_name
                  FROM DUAL) src
         ON (trg.stg_queue_code = src.queue_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.stg_queue_name = src.queue_name
         WHEN NOT MATCHED THEN
            INSERT (trg.stg_queue_code, trg.stg_queue_name)
            VALUES (src.queue_code, src.queue_name);
      COMMIT;
   END prc_queue_ins;

   FUNCTION fct_queue_finished (
      p_n_queue_id   NUMBER
   )
      RETURN BOOLEAN
   IS
      l_n_step_status_min   NUMBER;
   BEGIN
      SELECT MIN (etl_step_status)
        INTO l_n_step_status_min
        FROM stg_queue_object_t
       WHERE stg_queue_id = p_n_queue_id;

      IF l_n_step_status_min > 0
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END fct_queue_finished;

   FUNCTION fct_step_available (
      p_n_queue_id   NUMBER
   )
      RETURN BOOLEAN
   IS
      l_n_step_cnt   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_step_cnt
        FROM stg_queue_object_t
       WHERE etl_step_status = 0
         AND stg_queue_id = p_n_queue_id;

      IF l_n_step_cnt > 0
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END fct_step_available;

   PROCEDURE prc_enqueue_object (
      p_vc_queue_code    VARCHAR2
    , p_vc_source_code   VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name   VARCHAR2 DEFAULT 'ALL'
   )
   IS
      l_n_result    NUMBER;
      l_n_di_gui    NUMBER;
      l_n_step_no   NUMBER;
   BEGIN
       trc.log_info ('Enqueue all objects', 'Enqueue Begin');

      DELETE      stg_queue_object_t
            WHERE stg_queue_id IN (SELECT stg_queue_id
                                           FROM stg_queue_t
                                          WHERE stg_queue_code = p_vc_queue_code)
              AND stg_object_id IN (
                                    SELECT o.stg_object_id
                                      FROM stg_object_t o
                                         , stg_source_t s
                                     WHERE o.stg_source_id = s.stg_source_id
                                       AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                                       AND p_vc_object_name IN (o.stg_object_name, 'ALL'));

      INSERT INTO stg_queue_object_t
                  (stg_queue_id
                 , stg_object_id
                 , etl_step_status
                  )
         SELECT q.stg_queue_id
              , o.stg_object_id
              , 0
           FROM stg_object_t o
              , stg_source_t s
              , stg_queue_t q
          WHERE o.stg_source_id = s.stg_source_id
            AND q.stg_queue_code = p_vc_queue_code
            AND p_vc_source_code IN (s.stg_source_code, 'ALL')
            AND p_vc_object_name IN (o.stg_object_name, 'ALL');

      COMMIT;
       trc.log_info ('Enqueue all objects', 'Enqueue End');
   END prc_enqueue_object;

   PROCEDURE prc_enqueue_source (
      p_vc_source_code         VARCHAR2
    , p_n_threshold_tot_rows   NUMBER
   )
   IS
      l_n_tot_rows                 NUMBER        := 0;
      l_n_tot_rows_next_theshold   NUMBER        := 0;
      l_n_queue_order              NUMBER        := 0;
      l_vc_queue_code              VARCHAR2 (10);
   BEGIN
      l_n_tot_rows_next_theshold    := p_n_threshold_tot_rows;

      SELECT NVL (MAX (LTRIM (stg_queue_code, p_vc_source_code)) + 1, 0)
        INTO l_n_queue_order
        FROM stg_queue_t
       WHERE stg_queue_code LIKE p_vc_source_code || '%';

      l_vc_queue_code               := p_vc_source_code || TRIM (TO_CHAR (l_n_queue_order, '000'));
      prc_queue_ins (l_vc_queue_code, l_vc_queue_code);

      -- Order objects according to size in rows
      FOR r_obj IN (SELECT   o.stg_object_name
                           , t.num_rows
                        FROM stg_object_v o
                           , user_tables t
                       WHERE o.stg_stg2_table_name = t.table_name
                         AND stg_source_code = p_vc_source_code
                    ORDER BY t.num_rows)
      LOOP
         l_n_tot_rows    := l_n_tot_rows + r_obj.num_rows;

         -- If the threshold size is overtaken, then set next threshold and next queue
         IF l_n_tot_rows >= l_n_tot_rows_next_theshold
         THEN
            l_n_tot_rows_next_theshold    := l_n_tot_rows_next_theshold + p_n_threshold_tot_rows;
            l_n_queue_order               := l_n_queue_order + 1;
            l_vc_queue_code               := p_vc_source_code || TRIM (TO_CHAR (l_n_queue_order, '000'));
            prc_queue_ins (l_vc_queue_code, l_vc_queue_code);
         END IF;

         prc_enqueue_object (l_vc_queue_code
                           , p_vc_source_code
                           , r_obj.stg_object_name
                            );
      END LOOP;
   END prc_enqueue_source;

   PROCEDURE prc_execute_step (
      p_n_queue_id   NUMBER
   )
   IS
      l_vc_prc_name         type.vc_obj_plsql;
      l_n_object_id         NUMBER;
      l_vc_owner            type.vc_obj_plsql;
      l_vc_object           type.vc_obj_plsql;
      l_vc_package          type.vc_obj_plsql;
      l_vc_std_load_modus   type.vc_obj_plsql;
   BEGIN
       trc.log_info ('Queue ' || p_n_queue_id || ': Step Begin', 'Stream ' || p_n_queue_id || ': Step Begin');

      EXECUTE IMMEDIATE 'LOCK TABLE stg_queue_object_t IN EXCLUSIVE MODE WAIT 10';

      UPDATE    stg_queue_object_t
            SET etl_step_status = 1
              , etl_step_session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
              , etl_step_begin_date = SYSDATE
          WHERE stg_queue_object_id = (SELECT MIN (stg_queue_object_id)
                                               FROM stg_queue_object_t
                                              WHERE etl_step_status = 0
                                                AND stg_queue_id = p_n_queue_id)
      RETURNING stg_object_id
           INTO l_n_object_id;

      COMMIT;

      IF l_n_object_id IS NULL
      THEN
          trc.log_info ('Queue ' || p_n_queue_id || ': No steps available in queue', 'Queue ' || p_n_queue_id || ': Nothing to do');
      ELSE
         SELECT s.stg_owner
              , o.stg_object_name
              , o.stg_package_name
              , o.stg_std_load_modus
           INTO l_vc_owner
              , l_vc_object
              , l_vc_package
              , l_vc_std_load_modus
           FROM stg_source_t s
              , stg_object_t o
          WHERE s.stg_source_id = o.stg_source_id
            AND o.stg_object_id = l_n_object_id;

          trc.log_info ('Execute procedure ', 'Stream ' || p_n_queue_id || ': ');
         l_vc_prc_name    := l_vc_package || CASE
                                WHEN l_vc_std_load_modus = 'D'
                                   THEN '.prc_load_delta'
                                ELSE '.prc_load'
                             END;
          trc.log_info ('o=' || l_n_object_id || ' prc=' || l_vc_prc_name, 'Queue ' || p_n_queue_id);

         BEGIN
            EXECUTE IMMEDIATE 'BEGIN ' || l_vc_prc_name || '; END;';

             trc.log_info ('Queue ' || p_n_queue_id || ': Step executed', 'Queue ' || p_n_queue_id || ': Step executed');

            UPDATE stg_queue_object_t
               SET etl_step_status = 2
                 , etl_step_end_date = SYSDATE
             WHERE stg_object_id = l_n_object_id;
         EXCEPTION
            WHEN OTHERS
            THEN
                trc.log_info ('Queue ' || p_n_queue_id || ': Error', 'Queue ' || p_n_queue_id || ': Error');

               UPDATE stg_queue_object_t
                  SET etl_step_status = 3
                    , etl_step_end_date = SYSDATE
                WHERE stg_object_id = l_n_object_id;
         END;

         COMMIT;
          trc.log_info ('Queue ' || p_n_queue_id || ': End', 'Queue ' || p_n_queue_id || ': End');
      END IF;
   END prc_execute_step;

   PROCEDURE prc_execute_queue (
      p_vc_queue_code   VARCHAR2
   )
   IS
      l_n_out        NUMBER;
      l_n_di_gui     NUMBER;
      l_n_step_no    NUMBER;
      l_n_queue_id   NUMBER;
   BEGIN
      --stg_stat.prc_set_load_id;

      SELECT MAX (stg_queue_id)
        INTO l_n_queue_id
        FROM stg_queue_t
       WHERE stg_queue_code = p_vc_queue_code;

      IF l_n_queue_id IS NOT NULL
      THEN
          trc.log_info ('Execute single steps', 'Queue Begin');

         WHILE fct_queue_finished (l_n_queue_id) = FALSE
         LOOP
            IF fct_step_available (l_n_queue_id) = TRUE
            THEN
                trc.log_info ('Execute next available step', 'Step Begin');
               prc_execute_step (l_n_queue_id);
                trc.log_info ('Step executed', 'Step End');
            END IF;
         END LOOP;

          trc.log_info ('No more steps to execute', 'Stream End');
      ELSE
          trc.log_info ('Queue ' || p_vc_queue_code || ' doesn''t exist', 'Queue End');
      END IF;
   END prc_execute_queue;

   PROCEDURE prc_truncate_stg1 (
      p_vc_source_code   VARCHAR2
   )
   IS
   BEGIN
      FOR r_obj IN (SELECT stg_package_name
                      FROM stg_object_v
                     WHERE stg_source_code = p_vc_source_code)
      LOOP
         EXECUTE IMMEDIATE 'BEGIN ' || r_obj.stg_package_name || '.prc_trunc_stage1; END;';
      END LOOP;
   END;

   PROCEDURE prc_initialize_queue (
      p_vc_queue_code   VARCHAR2
   )
   IS
   BEGIN
      UPDATE stg_queue_object_t
         SET etl_step_status = 0
           , etl_step_session_id = NULL
           , etl_step_begin_date = NULL
           , etl_step_end_date = NULL
       WHERE stg_queue_id IN (SELECT stg_queue_id
                                      FROM stg_queue_t
                                     WHERE stg_queue_code = p_vc_queue_code);

      COMMIT;
   END prc_initialize_queue;

   PROCEDURE prc_bodi_stg1_job_init (
      p_vc_source_code                VARCHAR2
    , p_vc_object_name                VARCHAR2
    , p_stage_id                      NUMBER
    , p_vc_workflow_name              VARCHAR2
    , p_vc_repository_name            VARCHAR2
    , p_n_gui                IN OUT   NUMBER
    , p_n_stat_id            IN OUT   NUMBER
   )
   IS
      l_n_step_no   NUMBER;
      l_n_result    NUMBER;
      l_n_part_id   NUMBER;
   BEGIN
       trc.log_info ('Start'
                     , p_vc_workflow_name
                     , NULL
                     , NULL
                     , NULL
                      );

      -- Try to get partition id from workflow name (example JOB_IRB_ABC_R04 => 4)
      BEGIN
         -- Try with 2-digit partition id
         l_n_part_id    := TO_NUMBER (SUBSTR (TRIM (p_vc_workflow_name), -2));
      EXCEPTION
         WHEN OTHERS
         THEN
            BEGIN
               -- Try with 2-digit partition id
               l_n_part_id    := TO_NUMBER (SUBSTR (TRIM (p_vc_workflow_name), -1));
            EXCEPTION
               WHEN OTHERS
               THEN
                  NULL;
            -- No partition given
            END;
      END;

      p_n_stat_id    := stg_stat.prc_stat_begin (p_vc_source_code
                                                         , p_vc_object_name
                                                         , p_stage_id
                                                         , l_n_part_id
                                                         , 'INS'
                                                          );
   END prc_bodi_stg1_job_init;

   PROCEDURE prc_bodi_stg1_job_final (
      p_vc_workflow_name     VARCHAR2
    , p_vc_repository_name   VARCHAR2
    , p_n_gui                NUMBER
    , p_n_stat_id            NUMBER
   )
   IS
      l_n_result   NUMBER;
   BEGIN
      stg_stat.prc_stat_end (p_n_stat_id
                                     , 1
                                     , 0
                                      );
       trc.log_info ('Finish'
                     , p_vc_workflow_name
                     , NULL
                     , NULL
                     , NULL
                      );
   END prc_bodi_stg1_job_final;

   PROCEDURE prc_bodi_stg1_job_error (
      p_vc_workflow_name     VARCHAR2
    , p_vc_repository_name   VARCHAR2
    , p_n_gui                NUMBER
    , p_n_stat_id            NUMBER
   )
   IS
      l_n_step_no   NUMBER;
      l_n_result    NUMBER;
   BEGIN
      stg_stat.prc_stat_end (p_n_stat_id
                                     , 0
                                     , 1
                                      );
       trc.log_error ('Error'
                     , p_vc_workflow_name
                      );
   END prc_bodi_stg1_job_error;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: stg_ctl-impl.sql 2876 2012-06-20 13:27:31Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ctl/stg_ctl-impl.sql $';
END stg_ctl;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('stg_ctl'
                                 , 'stg_ctl'
                                 , TRUE
                                  );
END;
/

SHOW errors