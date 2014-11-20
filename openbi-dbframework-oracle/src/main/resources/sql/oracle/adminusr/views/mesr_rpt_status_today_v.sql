CREATE OR REPLACE FORCE VIEW p#frm#mesr_rpt_status_today_v
AS
     SELECT rep.ok AS result
          , rep.test_name AS step_code
          , rep.description AS step_name
          , rep.result_out AS actual
          , rep.min_t AS MIN
          , rep.max_t AS MAX
       FROM (  SELECT e.update_date AS date_dt
                    , CASE
                         WHEN e.mesr_exec_result_value >= t.mesr_threshold_min
                          AND e.mesr_exec_result_value <= t.mesr_threshold_max THEN
                            'OK'
                         ELSE
                            'ERROR'
                      END
                         AS ok
                    , s.mesr_query_id AS query_id
                    , e.mesr_exec_id AS exec_id
                    , s.mesr_query_code AS test_name
                    , s.mesr_query_name AS description
                    , e.mesr_exec_result_value AS result_out
                    , t.mesr_threshold_min AS min_t
                    , t.mesr_threshold_max AS max_t
                 FROM p#frm#mesr_query_t s
                    , p#frm#mesr_keyfigure_t k
                    , p#frm#mesr_threshold_t t
                    , p#frm#mesr_exec_t e
                WHERE 1 = 1
                  AND s.mesr_query_id = k.mesr_query_id
                  AND k.mesr_keyfigure_id = e.mesr_keyfigure_id
                  AND e.mesr_keyfigure_id = t.mesr_keyfigure_id
                  AND NVL (
                         t.mesr_threshold_from
                       , TO_DATE (
                            '10000101'
                          , 'yyyymmdd'
                         )
                      ) <= e.update_date
                  AND e.update_date <= NVL (
                                          t.mesr_threshold_to
                                        , TO_DATE (
                                             '99991231'
                                           , 'yyyymmdd'
                                          )
                                       )
                  AND TRUNC (e.update_date) = TRUNC (SYSDATE)
             ORDER BY s.mesr_query_id) rep
          , (  SELECT s.mesr_query_id AS query_id
                    , MAX (e.mesr_exec_id) AS exec_id
                 FROM p#frm#mesr_query_t s
                    , p#frm#mesr_keyfigure_t k
                    , p#frm#mesr_exec_t e
                WHERE 1 = 1
                  AND s.mesr_query_id = k.mesr_query_id
                  AND k.mesr_keyfigure_id = e.mesr_keyfigure_id
                  AND TRUNC (e.update_date) = TRUNC (SYSDATE)
             GROUP BY s.mesr_query_id) exec
      WHERE rep.query_id = exec.query_id
        AND rep.exec_id = exec.exec_id
   ORDER BY rep.ok
          , rep.query_id;