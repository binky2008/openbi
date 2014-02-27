CREATE OR REPLACE VIEW mes_exec_verify_v
AS
   SELECT   q.mes_query_code
          , q.mes_query_name
          , k.mes_keyfigure_code
          , k.mes_keyfigure_name
          , e.mes_exec_result_value
          , LEAD (e.mes_exec_result_value) OVER (PARTITION BY k.mes_keyfigure_id ORDER BY e.update_date DESC) As mes_exec_result_previous
          , t.mes_threshold_type
          , t.mes_threshold_min
          , t.mes_threshold_max
          , e.update_date AS execution_date
       FROM mes_query_t q
          , mes_keyfigure_t k
          , mes_threshold_t t
          , mes_exec_t e
      WHERE q.mes_query_id = k.mes_query_id(+)
        AND k.mes_keyfigure_id = e.mes_keyfigure_id(+)
        AND e.mes_keyfigure_id = t.mes_keyfigure_id(+)
        AND NVL (t.mes_threshold_from(+), TO_DATE ('01011111', 'ddmmyyyy')) <= e.update_date
        AND e.update_date <= NVL (t.mes_threshold_to(+), TO_DATE ('09099999', 'ddmmyyyy'))
   ORDER BY e.update_date DESC;

COMMENT ON TABLE mes_exec_verify_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_exec_verify_v TO PUBLIC;

begin
   utl_ddl.prc_create_synonym ('mes_exec_verify_v'
                                 , 'mes_exec_verify_v'
                                 , TRUE
                                  );
END;
/