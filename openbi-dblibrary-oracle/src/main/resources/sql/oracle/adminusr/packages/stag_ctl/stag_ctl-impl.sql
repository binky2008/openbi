CREATE OR REPLACE PACKAGE BODY p#frm#stag_ctl
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_ctl-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ctl/stag_ctl-impl.sql $
   */
   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   PROCEDURE prc_queue_ins (
      p_vc_queue_code    VARCHAR2
    , p_vc_queue_name    VARCHAR2
   )
   IS
   BEGIN
      MERGE INTO p#frm#stag_queue_t trg
           USING (SELECT p_vc_queue_code AS queue_code
                       , p_vc_queue_name AS queue_name
                    FROM DUAL) src
              ON (trg.stag_queue_code = src.queue_code)
      WHEN MATCHED THEN
         UPDATE SET trg.stag_queue_name = src.queue_name
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_queue_code
                     , trg.stag_queue_name
                    )
             VALUES (
                       src.queue_code
                     , src.queue_name
                    );

      COMMIT;
   END prc_queue_ins;

   FUNCTION fct_queue_finished (p_n_queue_id NUMBER)
      RETURN BOOLEAN
   IS
      l_n_step_status_min   NUMBER;
   BEGIN
      SELECT MIN (etl_step_status)
        INTO l_n_step_status_min
        FROM p#frm#stag_queue_object_t
       WHERE stag_queue_id = p_n_queue_id;

      IF l_n_step_status_min > 0 THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END fct_queue_finished;

   FUNCTION fct_step_available (p_n_queue_id NUMBER)
      RETURN BOOLEAN
   IS
      l_n_step_cnt   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO l_n_step_cnt
        FROM p#frm#stag_queue_object_t
       WHERE etl_step_status = 0
         AND stag_queue_id = p_n_queue_id;

      IF l_n_step_cnt > 0 THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   END fct_step_available;

   PROCEDURE prc_enqueue_object (
      p_vc_queue_code     VARCHAR2
    , p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
   )
   IS
      l_n_result    NUMBER;
      l_n_di_gui    NUMBER;
      l_n_step_no   NUMBER;
   BEGIN
      p#frm#trac.log_info (
         'Enqueue all objects'
       , 'Enqueue Begin'
      );

      DELETE p#frm#stag_queue_object_t
       WHERE stag_queue_id IN (SELECT stag_queue_id
                                 FROM p#frm#stag_queue_t
                                WHERE stag_queue_code = p_vc_queue_code)
         AND stag_object_id IN (SELECT o.stag_object_id
                                  FROM p#frm#stag_object_t o
                                     , p#frm#stag_source_t s
                                 WHERE o.stag_source_id = s.stag_source_id
                                   AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                                   AND p_vc_object_name IN (o.stag_object_name, 'ALL'));

      INSERT INTO p#frm#stag_queue_object_t (
                     stag_queue_id
                   , stag_object_id
                   , etl_step_status
                  )
         SELECT q.stag_queue_id
              , o.stag_object_id
              , 0
           FROM p#frm#stag_object_t o
              , p#frm#stag_source_t s
              , p#frm#stag_queue_t q
          WHERE o.stag_source_id = s.stag_source_id
            AND q.stag_queue_code = p_vc_queue_code
            AND p_vc_source_code IN (s.stag_source_code, 'ALL')
            AND p_vc_object_name IN (o.stag_object_name, 'ALL');

      COMMIT;
      p#frm#trac.log_info (
         'Enqueue all objects'
       , 'Enqueue End'
      );
   END prc_enqueue_object;

   PROCEDURE prc_enqueue_source (
      p_vc_source_code          VARCHAR2
    , p_n_threshold_tot_rows    NUMBER
   )
   IS
      l_n_tot_rows                 NUMBER := 0;
      l_n_tot_rows_next_theshold   NUMBER := 0;
      l_n_queue_order              NUMBER := 0;
      l_vc_queue_code              VARCHAR2 (10);
   BEGIN
      l_n_tot_rows_next_theshold := p_n_threshold_tot_rows;

      SELECT NVL (
                  MAX (LTRIM (
                          stag_queue_code
                        , p_vc_source_code
                       ))
                + 1
              , 0
             )
        INTO l_n_queue_order
        FROM p#frm#stag_queue_t
       WHERE stag_queue_code LIKE
                   p_vc_source_code
                || '%';

      l_vc_queue_code :=
            p_vc_source_code
         || TRIM (TO_CHAR (
                     l_n_queue_order
                   , '000'
                  ));
      prc_queue_ins (
         l_vc_queue_code
       , l_vc_queue_code
      );

      -- Order objects according to size in rows
      FOR r_obj IN (  SELECT o.stag_object_name
                           , t.num_rows
                        FROM p#frm#stag_object_v o
                           , user_tables t
                       WHERE o.stag_hist_table_name = t.table_name
                         AND stag_source_code = p_vc_source_code
                    ORDER BY t.num_rows) LOOP
         l_n_tot_rows :=
              l_n_tot_rows
            + r_obj.num_rows;

         -- If the threshold size is overtaken, then set next threshold and next queue
         IF l_n_tot_rows >= l_n_tot_rows_next_theshold THEN
            l_n_tot_rows_next_theshold :=
                 l_n_tot_rows_next_theshold
               + p_n_threshold_tot_rows;
            l_n_queue_order :=
                 l_n_queue_order
               + 1;
            l_vc_queue_code :=
                  p_vc_source_code
               || TRIM (TO_CHAR (
                           l_n_queue_order
                         , '000'
                        ));
            prc_queue_ins (
               l_vc_queue_code
             , l_vc_queue_code
            );
         END IF;

         prc_enqueue_object (
            l_vc_queue_code
          , p_vc_source_code
          , r_obj.stag_object_name
         );
      END LOOP;
   END prc_enqueue_source;

   PROCEDURE prc_execute_step (p_n_queue_id NUMBER)
   IS
      l_vc_prc_name         t_object_name := 'prc_execute_step';
      l_n_object_id         NUMBER;
      l_vc_owner            t_object_name;
      l_vc_object           t_object_name;
      l_vc_package          t_object_name;
      l_vc_std_load_modus   t_object_name;
   BEGIN
      p#frm#trac.log_info (
            'Queue '
         || p_n_queue_id
         || ': Step Begin'
       ,    'Stream '
         || p_n_queue_id
         || ': Step Begin'
      );

      EXECUTE IMMEDIATE 'LOCK TABLE stag_queue_object_t IN EXCLUSIVE MODE WAIT 10';

         UPDATE p#frm#stag_queue_object_t
            SET etl_step_status = 1
              , etl_step_session_id =
                   SYS_CONTEXT (
                      'USERENV'
                    , 'SESSIONID'
                   )
              , etl_step_begin_date = SYSDATE
          WHERE stag_queue_object_id = (SELECT MIN (stag_queue_object_id)
                                          FROM p#frm#stag_queue_object_t
                                         WHERE etl_step_status = 0
                                           AND stag_queue_id = p_n_queue_id)
      RETURNING stag_object_id
           INTO l_n_object_id;

      COMMIT;

      IF l_n_object_id IS NULL THEN
         p#frm#trac.log_info (
               'Queue '
            || p_n_queue_id
            || ': No steps available in queue'
          ,    'Queue '
            || p_n_queue_id
            || ': Nothing to do'
         );
      ELSE
         SELECT s.stag_owner
              , o.stag_object_name
              , o.stag_package_name
           INTO l_vc_owner
              , l_vc_object
              , l_vc_package
           FROM p#frm#stag_source_t s
              , p#frm#stag_object_t o
          WHERE s.stag_source_id = o.stag_source_id
            AND o.stag_object_id = l_n_object_id;

         p#frm#trac.log_info (
            'Execute procedure '
          ,    'Stream '
            || p_n_queue_id
            || ': '
         );
         l_vc_prc_name :=
               l_vc_package
            || CASE
                  WHEN l_vc_std_load_modus = 'D' THEN
                        '.'
                     || p#frm#stag_param.c_vc_procedure_wrapper_incr
                  ELSE
                        '.'
                     || p#frm#stag_param.c_vc_procedure_wrapper
               END;
         p#frm#trac.log_info (
               'o='
            || l_n_object_id
            || ' prc='
            || l_vc_prc_name
          ,    'Queue '
            || p_n_queue_id
         );

         BEGIN
            EXECUTE IMMEDIATE
                  'BEGIN '
               || l_vc_prc_name
               || '; END;';

            p#frm#trac.log_info (
                  'Queue '
               || p_n_queue_id
               || ': Step executed'
             ,    'Queue '
               || p_n_queue_id
               || ': Step executed'
            );

            UPDATE p#frm#stag_queue_object_t
               SET etl_step_status = 2
                 , etl_step_end_date = SYSDATE
             WHERE stag_object_id = l_n_object_id;
         EXCEPTION
            WHEN OTHERS THEN
               p#frm#trac.log_info (
                     'Queue '
                  || p_n_queue_id
                  || ': Error'
                ,    'Queue '
                  || p_n_queue_id
                  || ': Error'
               );

               UPDATE p#frm#stag_queue_object_t
                  SET etl_step_status = 3
                    , etl_step_end_date = SYSDATE
                WHERE stag_object_id = l_n_object_id;
         END;

         COMMIT;
         p#frm#trac.log_info (
               'Queue '
            || p_n_queue_id
            || ': End'
          ,    'Queue '
            || p_n_queue_id
            || ': End'
         );
      END IF;
   END prc_execute_step;

   PROCEDURE prc_execute_queue (p_vc_queue_code VARCHAR2)
   IS
      l_n_out        NUMBER;
      l_n_di_gui     NUMBER;
      l_n_step_no    NUMBER;
      l_n_queue_id   NUMBER;
   BEGIN
      --p#frm#stag_stat.prc_set_load_id;
      SELECT MAX (stag_queue_id)
        INTO l_n_queue_id
        FROM p#frm#stag_queue_t
       WHERE stag_queue_code = p_vc_queue_code;

      IF l_n_queue_id IS NOT NULL THEN
         p#frm#trac.log_info (
            'Execute single steps'
          , 'Queue Begin'
         );

         WHILE fct_queue_finished (l_n_queue_id) = FALSE LOOP
            IF fct_step_available (l_n_queue_id) = TRUE THEN
               p#frm#trac.log_info (
                  'Execute next available step'
                , 'Step Begin'
               );
               prc_execute_step (l_n_queue_id);
               p#frm#trac.log_info (
                  'Step executed'
                , 'Step End'
               );
            END IF;
         END LOOP;

         p#frm#trac.log_info (
            'No more steps to execute'
          , 'Stream End'
         );
      ELSE
         p#frm#trac.log_info (
               'Queue '
            || p_vc_queue_code
            || ' doesn''t exist'
          , 'Queue End'
         );
      END IF;
   END prc_execute_queue;

   PROCEDURE prc_truncate_stage (p_vc_source_code VARCHAR2)
   IS
   BEGIN
      FOR r_obj IN (SELECT stag_package_name
                      FROM p#frm#stag_object_v
                     WHERE stag_source_code = p_vc_source_code) LOOP
         EXECUTE IMMEDIATE
               'BEGIN '
            || r_obj.stag_package_name
            || '.prc_trunc_stage1; END;';
      END LOOP;
   END;

   PROCEDURE prc_initialize_queue (p_vc_queue_code VARCHAR2)
   IS
   BEGIN
      UPDATE p#frm#stag_queue_object_t
         SET etl_step_status = 0
           , etl_step_session_id = NULL
           , etl_step_begin_date = NULL
           , etl_step_end_date = NULL
       WHERE stag_queue_id IN (SELECT stag_queue_id
                                 FROM p#frm#stag_queue_t
                                WHERE stag_queue_code = p_vc_queue_code);

      COMMIT;
   END prc_initialize_queue;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: stag_ctl-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ctl/stag_ctl-impl.sql $';
END p#frm#stag_ctl;