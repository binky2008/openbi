CREATE OR REPLACE VIEW p#frm#mesr_keyfigure_v
AS
   SELECT q.mesr_query_code
        , k.mesr_keyfigure_id
        , k.mesr_keyfigure_code
        , k.mesr_keyfigure_name
        , k.update_date
     FROM p#frm#mesr_query_t q
        , p#frm#mesr_keyfigure_t k
    WHERE k.mesr_query_id = q.mesr_query_id;