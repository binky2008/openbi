CREATE OR REPLACE FORCE VIEW qc_rpt_status_today_v
AS
   SELECT   rep.ok AS RESULT
          , rep.test_name AS step_code
          , rep.description AS step_name
          , rep.result_out AS actual
          , rep.min_t AS MIN
          , rep.max_t AS MAX
       FROM (SELECT   e.update_date AS date_dt
                    , CASE
                         WHEN e.qc_exec_result_value >= t.qc_threshold_min
                         AND e.qc_exec_result_value <= t.qc_threshold_max
                            THEN 'OK'
                         ELSE 'ERROR'
                      END AS ok
                    , s.qc_step_id AS step_id
                    , e.qc_exec_id AS exec_id
                    , s.qc_step_code AS test_name
                    , s.qc_step_name AS description
                    , e.qc_exec_result_value AS result_out
                    , t.qc_threshold_min AS min_t
                    , t.qc_threshold_max AS max_t
                 FROM qc_step_t s
                    , qc_keyfigure_t k
                    , qc_threshold_t t
                    , qc_exec_t e
                WHERE 1 = 1
                  AND s.qc_step_id = k.qc_step_id
                  AND k.qc_keyfigure_id = e.qc_keyfigure_id
                  AND e.qc_keyfigure_id = t.qc_keyfigure_id
                  AND NVL (t.qc_threshold_from, TO_DATE ('01011111', 'ddmmyyyy')) <= e.update_date
                  AND e.update_date <= NVL (t.qc_threshold_to, TO_DATE ('09099999', 'ddmmyyyy'))
                  AND TRUNC (e.update_date) = TRUNC (SYSDATE)
             ORDER BY s.qc_step_id) rep
          , (SELECT   s.qc_step_id AS step_id
                    , MAX (e.qc_exec_id) AS exec_id
                 FROM qc_step_t s
                    , qc_keyfigure_t k
                    , qc_exec_t e
                WHERE 1 = 1
                  AND s.qc_step_id = k.qc_step_id
                  AND k.qc_keyfigure_id = e.qc_keyfigure_id
                  AND TRUNC (e.update_date) = TRUNC (SYSDATE)
             GROUP BY s.qc_step_id) exec
      WHERE rep.step_id = exec.step_id
        AND rep.exec_id = exec.exec_id
   ORDER BY rep.ok
          , rep.step_id;
          

COMMENT ON TABLE qc_rpt_status_today_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON qc_rpt_status_today_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('qc_rpt_status_today_v'
                                 , 'qc_rpt_status_today_v'
                                 , TRUE
                                  );
END;
/