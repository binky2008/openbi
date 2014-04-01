CREATE OR REPLACE VIEW mesr_exec_verify_v
AS
   SELECT   q.mesr_query_code
          , q.mesr_query_name
          , k.mesr_keyfigure_code
          , k.mesr_keyfigure_name
          , e.mesr_exec_result_value
          , LEAD (e.mesr_exec_result_value) OVER (PARTITION BY k.mesr_keyfigure_id ORDER BY e.update_date DESC) As mesr_exec_result_previous
          , t.mesr_threshold_type
          , t.mesr_threshold_min
          , t.mesr_threshold_max
          , e.update_date AS execution_date
       FROM mesr_query_t q
          , mesr_keyfigure_t k
          , mesr_threshold_t t
          , mesr_exec_t e
      WHERE q.mesr_query_id = k.mesr_query_id(+)
        AND k.mesr_keyfigure_id = e.mesr_keyfigure_id(+)
        AND e.mesr_keyfigure_id = t.mesr_keyfigure_id(+)
        AND NVL (t.mesr_threshold_from(+), TO_DATE ('01011111', 'ddmmyyyy')) <= e.update_date
        AND e.update_date <= NVL (t.mesr_threshold_to(+), TO_DATE ('09099999', 'ddmmyyyy'))
   ORDER BY e.update_date DESC;

COMMENT ON TABLE mesr_exec_verify_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mesr_exec_verify_v TO PUBLIC;