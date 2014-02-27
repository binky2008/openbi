BEGIN
   utl_ddl.prc_create_object_standard ('etl_stage_queue'
                                         , 'etl_stage_queue_code VARCHAR2(10),
                                            etl_stage_queue_name VARCHAR2(100)'
                                         , 'DROP'
                                         , TRUE
                                         , TRUE
                                          );
END;
/

ALTER TABLE etl_stage_queue_t ADD CONSTRAINT etl_stage_queue_uk UNIQUE (etl_stage_queue_code);

COMMENT ON TABLE etl_stage_queue_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';