CREATE OR REPLACE VIEW p#frm#mesr_threshold_v
AS
     SELECT q.mesr_query_code
          , q.mesr_query_name
          , q.mesr_query_sql
          , k.mesr_keyfigure_code
          , k.mesr_keyfigure_name
          , t.mesr_threshold_type
          , t.mesr_threshold_from
          , t.mesr_threshold_to
          , t.mesr_threshold_min
          , t.mesr_threshold_max
       FROM p#frm#mesr_query_t q
          , p#frm#mesr_keyfigure_t k
          , p#frm#mesr_threshold_t t
      WHERE q.mesr_query_id = k.mesr_query_id
        AND k.mesr_keyfigure_id = t.mesr_keyfigure_id
   ORDER BY q.mesr_query_id DESC
          , k.mesr_keyfigure_id DESC
          , t.mesr_threshold_id DESC;