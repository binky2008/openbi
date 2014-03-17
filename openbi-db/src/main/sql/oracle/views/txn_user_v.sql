CREATE OR REPLACE VIEW txn_user_v
AS
   SELECT ut.txn_user_id
        , ut.user_id
        , ut.txn_id
        , us.user_code
        , us.user_email
        , ta.txn_code
     from txn_user_t ut
        , user_t us
        , txn_t ta
    WHERE ut.user_id = us.user_id
      AND ut.txn_id = ta.txn_id;

COMMENT ON TABLE txn_user_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON txn_user_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('txn_user_v'
                             , 'txn_user_v'
                             , TRUE
                             );
END;
/