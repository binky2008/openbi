CREATE OR REPLACE VIEW trc_v
AS
   SELECT   *
       from trc_t
   ORDER BY trc_id DESC;

COMMENT ON TABLE trc_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON trc_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('trc_v'
                          , 'trc_v'
                          , TRUE
                          );
END;
/