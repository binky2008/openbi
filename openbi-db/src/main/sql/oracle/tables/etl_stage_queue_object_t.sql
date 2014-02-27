BEGIN
   utl_ddl.prc_create_object_standard ('etl_stage_queue_object'
                                         , 'etl_stage_queue_id NUMBER
                                            , etl_stage_object_id NUMBER
                                            , etl_step_status NUMBER
                                            , etl_step_session_id NUMBER
                                            , etl_step_begin_date DATE
                                            , etl_step_end_date DATE'
                                         , 'DROP'
                                         , TRUE
                                         , TRUE
                                          );
END;
/

ALTER TABLE etl_stage_queue_object_t ADD CONSTRAINT etl_stage_queue_object_uk UNIQUE (etl_stage_queue_id,etl_stage_object_id);

COMMENT ON TABLE etl_stage_queue_object_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';