CREATE OR REPLACE VIEW mes_query_v
AS
   SELECT mes_query_id
        , mes_query_code
        , mes_query_name
        , txn_layer_code
        , txn_entity_code
        , txn_environment_code
        , q.update_date
     FROM mes_query_t q
        , txn_layer_t l
        , txn_entity_t e
        , txn_environment_t n
    WHERE q.txn_entity_id = e.txn_entity_id
      AND q.txn_layer_id = l.txn_layer_id
      AND q.txn_environment_id = n.txn_environment_id;

COMMENT ON TABLE mes_query_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_query_v TO PUBLIC;

BEGIN
   utl_ddl.prc_create_synonym ('mes_query_v'
                              , 'mes_query_v'
                              , TRUE
                              );
END;
/