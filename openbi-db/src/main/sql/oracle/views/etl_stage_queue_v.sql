CREATE OR REPLACE VIEW etl_stage_queue_v
AS
   SELECT etl_stage_queue_id
        , etl_stage_queue_code
        , etl_stage_queue_name
     FROM etl_stage_queue_t q;

COMMENT ON TABLE etl_stage_queue_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_queue_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_queue_v'
                                 , 'etl_stage_queue_v'
                                 , TRUE
                                  );
END;
/