CREATE OR REPLACE VIEW taxn_v
AS
   SELECT     LEVEL AS taxn_level
            , taxn_id
            , taxn_code
            , taxn_name
            , SYS_CONNECT_BY_PATH (taxn_code, '/') taxn_path
         FROM taxn_t
   START WITH taxn_parent_id IS NULL
   CONNECT BY PRIOR taxn_id = taxn_parent_id;

COMMENT ON TABLE taxn_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON taxn_v TO PUBLIC;