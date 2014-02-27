CREATE OR REPLACE VIEW txn_taxonomy_v
AS
   SELECT     LEVEL AS txn_taxonomy_level
            , txn_taxonomy_id
            , txn_taxonomy_code
            , txn_taxonomy_name
            , SYS_CONNECT_BY_PATH (txn_taxonomy_code, '/') txn_taxonomy_path
         FROM txn_taxonomy_t
   START WITH txn_taxonomy_parent_id IS NULL
   CONNECT BY PRIOR txn_taxonomy_id = txn_taxonomy_parent_id;

COMMENT ON TABLE txn_taxonomy_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON txn_taxonomy_v TO PUBLIC;

BEGIN
   aux_ddl.prc_create_synonym ('txn_taxonomy_v'
                             , 'txn_taxonomy_v'
                             , TRUE
                             );
END;
/