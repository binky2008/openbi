CREATE OR REPLACE VIEW txn_v
AS
   SELECT     LEVEL AS txn_level
            , txn_id
            , txn_code
            , txn_name
            , SYS_CONNECT_BY_PATH (txn_code, '/') txn_path
         FROM txn_t
   START WITH txn_parent_id IS NULL
   CONNECT BY PRIOR txn_id = txn_parent_id;

COMMENT ON TABLE txn_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON txn_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('txn_v'
                             , 'txn_v'
                             , TRUE
                             );
END;
/