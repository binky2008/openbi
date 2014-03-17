BEGIN
   ddl.prc_create_entity ('stg_queue'
                                         , 'stg_queue_code VARCHAR2(10),
                                            stg_queue_name VARCHAR2(100)'
                                         , 'DROP'
                                         , TRUE
                                         , TRUE
                                          );
END;
/

ALTER TABLE stg_queue_t ADD CONSTRAINT stg_queue_uk UNIQUE (stg_queue_code);

COMMENT ON TABLE stg_queue_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';