CREATE OR REPLACE VIEW p#frm#mesr_query_v
AS
   SELECT mesr_query_id
        , mesr_query_code
        , mesr_query_name
        , update_date
     FROM p#frm#mesr_query_t;