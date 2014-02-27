CREATE OR REPLACE VIEW txn_user_v
AS
   SELECT ut.txn_user_id
        , ut.aux_user_id
        , ut.txn_taxonomy_id
        , us.aux_user_code
        , us.aux_user_email
        , ta.txn_taxonomy_code
     from txn_user_t ut
        , aux_user_t us
        , txn_taxonomy_t ta
    WHERE ut.txn_user_id = us.aux_user_id
      AND ut.txn_taxonomy_id = ta.txn_taxonomy_id;

COMMENT ON TABLE txn_user_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON txn_user_v TO PUBLIC;

BEGIN
   aux_ddl.prc_create_synonym ('txn_user_v'
                             , 'txn_user_v'
                             , TRUE
                             );
END;
/