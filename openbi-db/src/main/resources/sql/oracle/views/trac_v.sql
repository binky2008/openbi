CREATE OR REPLACE VIEW trac_v
AS
   SELECT   *
       from trac_t
   ORDER BY trac_id DESC;

COMMENT ON TABLE trac_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON trac_v TO PUBLIC;