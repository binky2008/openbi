CREATE OR REPLACE PACKAGE BODY pkg_etl_stage_ctl
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-06-20 15:27:31 +0200 (Mi, 20 Jun 2012) $
    * $Revision: 2876 $
    * $Id: pkg_etl_stage_ctl-impl.sql 2876 2012-06-20 13:27:31Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_ctl/pkg_etl_stage_ctl-impl.sql $
    */
   PROCEDURE prc_queue_ins (
      p_vc_queue_code   VARCHAR2
    , p_vc_queue_name   VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO etl_stage_queue_t trg
         USING (SELECT p_vc_queue_code AS queue_code
                     , p_vc_queue_name AS queue_name
                  FROM DUAL) src
         ON (trg.etl_stage_queue_code = src.queue_code)
         WHEN MATCHED THEN
            UPDATE
               SET trg.etl_stage_queue_name = src.queue_name
         WHEN NOT MATCHED THEN
            INSERT (trg.etl_stage_queue_code, trg.etl_stage_queue_name)
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
        FROM etl_stage_queue_object_t
       WHERE etl_stage_queue_id = p_n_queue_id;

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
        FROM etl_stage_queue_object_t
       WHERE etl_step_status = 0
         AND etl_stage_queue_id = p_n_queue_id;

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
      l_n_result    := pkg_utl_job.initialize ('STAGE_ENQUEUE'
                                             , 'STAGE'
                                             , l_n_di_gui
                                             , l_n_step_no
                                              );
      l_n_result    := pkg_utl_job.set_step_no ('STAGE_ENQUEUE'
                                              , 'STAGE'
                                              , l_n_di_gui
                                              , 0
                                              , 'BEGIN'
                                               );
      pkg_utl_log.LOG ('Enqueue all objects', 'Enqueue Begin');

      DELETE      etl_stage_queue_object_t
            WHERE etl_stage_queue_id IN (SELECT etl_stage_queue_id
                                           FROM etl_stage_queue_t
                                          WHERE etl_stage_queue_code = p_vc_queue_code)
              AND etl_stage_object_id IN (
                                    SELECT o.etl_stage_object_id
                                      FROM etl_stage_object_t o
                                         , etl_stage_source_t s
                                     WHERE o.etl_stage_source_id = s.etl_stage_source_id
                                       AND p_vc_source_code IN (s.etl_stage_source_code, 'ALL')
                                       AND p_vc_object_name IN (o.etl_stage_object_name, 'ALL'));

      INSERT INTO etl_stage_queue_object_t
                  (etl_stage_queue_id
                 , etl_stage_object_id
                 , etl_step_status
                  )
         SELECT q.etl_stage_queue_id
              , o.etl_stage_object_id
              , 0
           FROM etl_stage_object_t o
              , etl_stage_source_t s
              , etl_stage_queue_t q
          WHERE o.etl_stage_source_id = s.etl_stage_source_id
            AND q.etl_stage_queue_code = p_vc_queue_code
            AND p_vc_source_code IN (s.etl_stage_source_code, 'ALL')
            AND p_vc_object_name IN (o.etl_stage_object_name, 'ALL');

      COMMIT;
      pkg_utl_log.LOG ('Enqueue all objects', 'Enqueue End');
      l_n_result    := pkg_utl_job.set_step_no ('STAGE_ENQUEUE'
                                              , 'STAGE'
                                              , l_n_di_gui
                                              , 1
                                              , 'END'
                                               );
      l_n_result    := pkg_utl_job.finalize ('STAGE_ENQUEUE'
                                           , 'STAGE'
                                           , l_n_di_gui
                                            );
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

      SELECT NVL (MAX (LTRIM (etl_stage_queue_code, p_vc_source_code)) + 1, 0)
        INTO l_n_queue_order
        FROM etl_stage_queue_t
       WHERE etl_stage_queue_code LIKE p_vc_source_code || '%';

      l_vc_queue_code               := p_vc_source_code || TRIM (TO_CHAR (l_n_queue_order, '000'));
      prc_queue_ins (l_vc_queue_code, l_vc_queue_code);

      -- Order objects according to size in rows
      FOR r_obj IN (SELECT   o.etl_stage_object_name
                           , t.num_rows
                        FROM etl_stage_object_v o
                           , user_tables t
                       WHERE o.etl_stage_stg2_table_name = t.table_name
                         AND etl_stage_source_code = p_vc_source_code
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
                           , r_obj.etl_stage_object_name
                            );
      END LOOP;
   END prc_enqueue_source;

   PROCEDURE prc_execute_step (
      p_n_queue_id   NUMBER
   )
   IS
      l_vc_prc_name         pkg_utl_type.vc_obj_plsql;
      l_n_object_id         NUMBER;
      l_vc_owner            pkg_utl_type.vc_obj_plsql;
      l_vc_object           pkg_utl_type.vc_obj_plsql;
      l_vc_package          pkg_utl_type.vc_obj_plsql;
      l_vc_std_load_modus   pkg_utl_type.vc_obj_plsql;
   BEGIN
      pkg_utl_log.LOG ('Queue ' || p_n_queue_id || ': Step Begin', 'Stream ' || p_n_queue_id || ': Step Begin');

      EXECUTE IMMEDIATE 'LOCK TABLE etl_stage_queue_object_t IN EXCLUSIVE MODE WAIT 10';

      UPDATE    etl_stage_queue_object_t
            SET etl_step_status = 1
              , etl_step_session_id = SYS_CONTEXT ('USERENV', 'SESSIONID')
              , etl_step_begin_date = SYSDATE
          WHERE etl_stage_queue_object_id = (SELECT MIN (etl_stage_queue_object_id)
                                               FROM etl_stage_queue_object_t
                                              WHERE etl_step_status = 0
                                                AND etl_stage_queue_id = p_n_queue_id)
      RETURNING etl_stage_object_id
           INTO l_n_object_id;

      COMMIT;

      IF l_n_object_id IS NULL
      THEN
         pkg_utl_log.LOG ('Queue ' || p_n_queue_id || ': No steps available in queue', 'Queue ' || p_n_queue_id || ': Nothing to do');
      ELSE
         SELECT s.etl_stage_owner
              , o.etl_stage_object_name
              , o.etl_stage_package_name
              , o.etl_stage_std_load_modus
           INTO l_vc_owner
              , l_vc_object
              , l_vc_package
              , l_vc_std_load_modus
           FROM etl_stage_source_t s
              , etl_stage_object_t o
          WHERE s.etl_stage_source_id = o.etl_stage_source_id
            AND o.etl_stage_object_id = l_n_object_id;

         pkg_utl_log.LOG ('Execute procedure ', 'Stream ' || p_n_queue_id || ': ');
         l_vc_prc_name    := l_vc_package || CASE
                                WHEN l_vc_std_load_modus = 'D'
                                   THEN '.prc_load_delta'
                                ELSE '.prc_load'
                             END;
         pkg_utl_log.LOG ('o=' || l_n_object_id || ' prc=' || l_vc_prc_name, 'Queue ' || p_n_queue_id);

         BEGIN
            EXECUTE IMMEDIATE 'BEGIN ' || l_vc_prc_name || '; END;';

            pkg_utl_log.LOG ('Queue ' || p_n_queue_id || ': Step executed', 'Queue ' || p_n_queue_id || ': Step executed');

            UPDATE etl_stage_queue_object_t
               SET etl_step_status = 2
                 , etl_step_end_date = SYSDATE
             WHERE etl_stage_object_id = l_n_object_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               pkg_utl_log.LOG ('Queue ' || p_n_queue_id || ': Error', 'Queue ' || p_n_queue_id || ': Error');

               UPDATE etl_stage_queue_object_t
                  SET etl_step_status = 3
                    , etl_step_end_date = SYSDATE
                WHERE etl_stage_object_id = l_n_object_id;
         END;

         COMMIT;
         pkg_utl_log.LOG ('Queue ' || p_n_queue_id || ': End', 'Queue ' || p_n_queue_id || ': End');
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
      pkg_etl_stage_stat.prc_set_load_id;

      SELECT MAX (etl_stage_queue_id)
        INTO l_n_queue_id
        FROM etl_stage_queue_t
       WHERE etl_stage_queue_code = p_vc_queue_code;

      IF l_n_queue_id IS NOT NULL
      THEN
         l_n_out    := pkg_utl_job.initialize ('STAGE_QUEUE_' || p_vc_queue_code
                                             , 'STAGE'
                                             , l_n_di_gui
                                             , l_n_step_no
                                              );
         l_n_out    := pkg_utl_job.set_step_no ('STAGE_QUEUE_' || p_vc_queue_code
                                              , 'STAGE'
                                              , l_n_di_gui
                                              , 0
                                              , 'BEGIN'
                                               );
         pkg_utl_log.LOG ('Execute single steps', 'Queue Begin');

         WHILE fct_queue_finished (l_n_queue_id) = FALSE
         LOOP
            IF fct_step_available (l_n_queue_id) = TRUE
            THEN
               pkg_utl_log.LOG ('Execute next available step', 'Step Begin');
               prc_execute_step (l_n_queue_id);
               pkg_utl_log.LOG ('Step executed', 'Step End');
            END IF;
         END LOOP;

         pkg_utl_log.LOG ('No more steps to execute', 'Stream End');
         l_n_out    := pkg_utl_job.set_step_no ('STAGE_QUEUE_' || p_vc_queue_code
                                              , 'STAGE'
                                              , l_n_di_gui
                                              , 1
                                              , 'END'
                                               );
         l_n_out    := pkg_utl_job.finalize ('STAGE_QUEUE_' || p_vc_queue_code
                                           , 'STAGE'
                                           , l_n_di_gui
                                            );
      ELSE
         pkg_utl_log.LOG ('Queue ' || p_vc_queue_code || ' doesn''t exist', 'Queue End');
      END IF;
   END prc_execute_queue;

   PROCEDURE prc_truncate_stg1 (
      p_vc_source_code   VARCHAR2
   )
   IS
   BEGIN
      FOR r_obj IN (SELECT etl_stage_package_name
                      FROM etl_stage_object_v
                     WHERE etl_stage_source_code = p_vc_source_code)
      LOOP
         EXECUTE IMMEDIATE 'BEGIN ' || r_obj.etl_stage_package_name || '.prc_trunc_stage1; END;';
      END LOOP;
   END;

   PROCEDURE prc_initialize_queue (
      p_vc_queue_code   VARCHAR2
   )
   IS
   BEGIN
      UPDATE etl_stage_queue_object_t
         SET etl_step_status = 0
           , etl_step_session_id = NULL
           , etl_step_begin_date = NULL
           , etl_step_end_date = NULL
       WHERE etl_stage_queue_id IN (SELECT etl_stage_queue_id
                                      FROM etl_stage_queue_t
                                     WHERE etl_stage_queue_code = p_vc_queue_code);

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
      pkg_utl_log.set_workflow_name (p_vc_workflow_name);
      l_n_result     := pkg_utl_job.initialize (p_vc_workflow_name
                                              , p_vc_repository_name
                                              , p_n_gui
                                              , l_n_step_no
                                              , NULL
                                               );
      l_n_result     := pkg_utl_job.set_step_no (p_vc_workflow_name
                                               , p_vc_repository_name
                                               , p_n_gui
                                               , 0
                                               , 'BEGIN'
                                                );
      pkg_utl_log.LOG ('Start'
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

      p_n_stat_id    := pkg_etl_stage_stat.prc_stat_begin (p_vc_source_code
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
      pkg_etl_stage_stat.prc_stat_end (p_n_stat_id
                                     , 1
                                     , 0
                                      );
      pkg_utl_log.LOG ('Finish'
                     , p_vc_workflow_name
                     , NULL
                     , NULL
                     , NULL
                      );
      l_n_result    := pkg_utl_job.set_step_no (p_vc_workflow_name
                                              , p_vc_repository_name
                                              , p_n_gui
                                              , 0
                                              , 'END'
                                               );
      l_n_result    := pkg_utl_job.finalize (p_vc_workflow_name
                                           , p_vc_repository_name
                                           , p_n_gui
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
      pkg_etl_stage_stat.prc_stat_end (p_n_stat_id
                                     , 0
                                     , 1
                                      );
      pkg_utl_log.LOG ('Error'
                     , p_vc_workflow_name
                     , 2
                     , NULL
                     , NULL
                      );
      l_n_result    := pkg_utl_job.set_error_status (p_vc_workflow_name
                                                   , p_vc_repository_name
                                                   , p_n_gui
                                                   , -20000
                                                    );
   END prc_bodi_stg1_job_error;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_etl_stage_ctl-impl.sql 2876 2012-06-20 13:27:31Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_ctl/pkg_etl_stage_ctl-impl.sql $';
END pkg_etl_stage_ctl;
/

SHOW errors

BEGIN
   pkg_utl_ddl.prc_create_synonym ('pkg_etl_stage_ctl'
                                 , 'pkg_etl_stage_ctl'
                                 , TRUE
                                  );
END;
/

SHOW errors