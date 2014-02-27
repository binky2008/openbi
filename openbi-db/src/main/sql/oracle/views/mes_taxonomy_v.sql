CREATE OR REPLACE VIEW mes_taxonomy_v
AS
   SELECT mt.mes_query_id
        , mt.txn_taxonomy_id
        , qu.mes_query_code
        , ta.txn_taxonomy_code
     FROM mes_taxonomy_t mt
        , mes_query_t qu
        , txn_taxonomy_t ta
    WHERE mt.mes_query_id = qu.mes_query_id
      AND mt.txn_taxonomy_id = ta.txn_taxonomy_id;

COMMENT ON TABLE mes_taxonomy_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON mes_taxonomy_v TO PUBLIC;

BEGIN
   utl_ddl.prc_create_synonym ('mes_taxonomy_v'
                             , 'mes_taxonomy_v'
                             , TRUE
                            );
END;
/