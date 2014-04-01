BEGIN
   ddls.prc_create_entity ('stag_queue_object'
                                         , 'stag_queue_id NUMBER
                                            , stag_object_id NUMBER
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

ALTER TABLE stag_queue_object_t ADD CONSTRAINT stag_queue_object_uk UNIQUE (stag_queue_id,stag_object_id);

COMMENT ON TABLE stag_queue_object_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';