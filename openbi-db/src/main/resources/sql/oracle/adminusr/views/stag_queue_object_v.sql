CREATE OR REPLACE VIEW stag_queue_object_v
AS
     SELECT q.stag_queue_code
          , qo.stag_object_id
          , s.stag_source_code
          , o.stag_object_name
          , qo.etl_step_status
          , qo.etl_step_session_id
          , qo.etl_step_begin_date AS step_begin
          , qo.etl_step_end_date AS step_finish
          , NUMTODSINTERVAL (
                 qo.etl_step_end_date
               - qo.etl_step_begin_date
             , 'day'
            )
               step_duration
       FROM stag_queue_object_t qo
          , stag_queue_t q
          , stag_object_t o
          , stag_source_t s
      WHERE qo.stag_queue_id = q.stag_queue_id
        AND qo.stag_object_id = o.stag_object_id
        AND o.stag_source_id = s.stag_source_id
   ORDER BY qo.etl_step_begin_date DESC NULLS FIRST
          , qo.etl_step_end_date DESC NULLS FIRST
          , qo.stag_queue_id DESC
          , qo.stag_queue_object_id;