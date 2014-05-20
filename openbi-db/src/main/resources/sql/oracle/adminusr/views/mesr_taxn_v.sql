CREATE OR REPLACE VIEW p#frm#mesr_taxn_v
AS
   SELECT mt.mesr_query_id
        , mt.taxn_id
        , qu.mesr_query_code
        , ta.taxn_code
     FROM p#frm#mesr_taxn_t mt
        , p#frm#mesr_query_t qu
        , p#frm#taxn_t ta
    WHERE mt.mesr_query_id = qu.mesr_query_id
      AND mt.taxn_id = ta.taxn_id;