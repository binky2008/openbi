CREATE OR REPLACE VIEW mes_txn_v
AS
   SELECT mt.mes_query_id
        , mt.txn_id
        , qu.mes_query_code
        , ta.txn_code
     FROM mes_txn_t mt
        , mes_query_t qu
        , txn_t ta
    WHERE mt.mes_query_id = qu.mes_query_id
      AND mt.txn_id = ta.txn_id;

COMMENT ON TABLE mes_txn_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_txn_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('mes_txn_v'
                             , 'mes_txn_v'
                             , TRUE
                            );
END;
/