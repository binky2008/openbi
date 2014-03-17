CREATE OR REPLACE VIEW mes_keyfigure_v
AS
   SELECT q.mes_query_code
        , k.mes_keyfigure_id
        , k.mes_keyfigure_code
        , k.mes_keyfigure_name
        , k.update_date
     FROM mes_query_t q
        , mes_keyfigure_t k
    WHERE k.mes_query_id = q.mes_query_id;

COMMENT ON TABLE mes_keyfigure_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_keyfigure_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('mes_keyfigure_v'
                             , 'mes_keyfigure_v'
                             , TRUE
                              );
END;
/