CREATE OR REPLACE VIEW mesr_keyfigure_v
AS
   SELECT q.mesr_query_code
        , k.mesr_keyfigure_id
        , k.mesr_keyfigure_code
        , k.mesr_keyfigure_name
        , k.update_date
     FROM mesr_query_t q
        , mesr_keyfigure_t k
    WHERE k.mesr_query_id = q.mesr_query_id;

COMMENT ON TABLE mesr_keyfigure_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mesr_keyfigure_v TO PUBLIC;