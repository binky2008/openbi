CREATE OR REPLACE VIEW mesr_taxn_v
AS
   SELECT mt.mesr_query_id
        , mt.taxn_id
        , qu.mesr_query_code
        , ta.taxn_code
     FROM mesr_taxn_t mt
        , mesr_query_t qu
        , taxn_t ta
    WHERE mt.mesr_query_id = qu.mesr_query_id
      AND mt.taxn_id = ta.taxn_id;

COMMENT ON TABLE mesr_taxn_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mesr_taxn_v TO PUBLIC;