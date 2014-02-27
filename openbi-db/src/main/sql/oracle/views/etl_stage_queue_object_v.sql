CREATE OR REPLACE VIEW etl_stage_queue_object_v
AS
   SELECT   q.etl_stage_queue_code
          , qo.etl_stage_object_id
          , s.etl_stage_source_code
          , o.etl_stage_object_name
          , qo.etl_step_status
          , qo.etl_step_session_id
          , qo.etl_step_begin_date AS step_begin
          , qo.etl_step_end_date AS step_finish
          , NUMTODSINTERVAL (qo.etl_step_end_date - qo.etl_step_begin_date, 'day') step_duration
       FROM etl_stage_queue_object_t qo
          , etl_stage_queue_t q
          , etl_stage_object_t o
          , etl_stage_source_t s
      WHERE qo.etl_stage_queue_id = q.etl_stage_queue_id
        AND qo.etl_stage_object_id = o.etl_stage_object_id
        AND o.etl_stage_source_id = s.etl_stage_source_id
   ORDER BY qo.etl_step_begin_date DESC NULLS FIRST
          , qo.etl_step_end_date DESC NULLS FIRST
          , qo.etl_stage_queue_id DESC
          , qo.etl_stage_queue_object_id;

COMMENT ON TABLE etl_stage_queue_object_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_queue_object_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_queue_object_v'
                                 , 'etl_stage_queue_object_v'
                                 , TRUE
                                  );
END;
/