CREATE OR REPLACE VIEW stg_queue_v
AS
   SELECT stg_queue_id
        , stg_queue_code
        , stg_queue_name
     FROM stg_queue_t q;

COMMENT ON TABLE stg_queue_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_queue_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_queue_v'
                                 , 'stg_queue_v'
                                 , TRUE
                                  );
END;
/