CREATE OR REPLACE VIEW stg_queue_object_v
AS
   SELECT   q.stg_queue_code
          , qo.stg_object_id
          , s.stg_source_code
          , o.stg_object_name
          , qo.etl_step_status
          , qo.etl_step_session_id
          , qo.etl_step_begin_date AS step_begin
          , qo.etl_step_end_date AS step_finish
          , NUMTODSINTERVAL (qo.etl_step_end_date - qo.etl_step_begin_date, 'day') step_duration
       FROM stg_queue_object_t qo
          , stg_queue_t q
          , stg_object_t o
          , stg_source_t s
      WHERE qo.stg_queue_id = q.stg_queue_id
        AND qo.stg_object_id = o.stg_object_id
        AND o.stg_source_id = s.stg_source_id
   ORDER BY qo.etl_step_begin_date DESC NULLS FIRST
          , qo.etl_step_end_date DESC NULLS FIRST
          , qo.stg_queue_id DESC
          , qo.stg_queue_object_id;

COMMENT ON TABLE stg_queue_object_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_queue_object_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_queue_object_v'
                                 , 'stg_queue_object_v'
                                 , TRUE
                                  );
END;
/