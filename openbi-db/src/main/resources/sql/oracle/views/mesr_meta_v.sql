CREATE OR REPLACE VIEW mesr_meta_v
AS
     SELECT s.mesr_query_code
          , s.mesr_query_name
          , s.mesr_query_sql
          , k.mesr_keyfigure_code
          , k.mesr_keyfigure_name
          , t.mesr_threshold_type
          , t.mesr_threshold_from
          , t.mesr_threshold_to
          , t.mesr_threshold_min
          , t.mesr_threshold_max
       FROM mesr_query_t s
          , mesr_keyfigure_t k
          , mesr_threshold_t t
      WHERE s.mesr_query_id = k.mesr_query_id(+)
        AND k.mesr_keyfigure_id = t.mesr_keyfigure_id(+)
   ORDER BY s.mesr_query_id DESC
          , k.mesr_keyfigure_id DESC
          , t.mesr_threshold_id DESC;