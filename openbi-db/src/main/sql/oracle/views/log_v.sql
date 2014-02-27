CREATE OR REPLACE VIEW aux_log_v
AS
   SELECT   *
       from aux_log_t
   ORDER BY aux_log_ID DESC;

COMMENT ON TABLE aux_log_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON aux_log_v TO PUBLIC;

BEGIN
   aux_ddl.prc_create_synonym ('aux_log_v'
                             , 'aux_log_v'
                             , TRUE
                              );
END;
/