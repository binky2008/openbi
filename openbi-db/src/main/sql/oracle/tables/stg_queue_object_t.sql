BEGIN
   ddl.prc_create_entity ('stg_queue_object'
                                         , 'stg_queue_id NUMBER
                                            , stg_object_id NUMBER
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

ALTER TABLE stg_queue_object_t ADD CONSTRAINT stg_queue_object_uk UNIQUE (stg_queue_id,stg_object_id);

COMMENT ON TABLE stg_queue_object_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';