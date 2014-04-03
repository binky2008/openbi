CREATE OR REPLACE VIEW mesr_query_v
AS
   SELECT mesr_query_id
        , mesr_query_code
        , mesr_query_name
        , update_date
     FROM mesr_query_t;