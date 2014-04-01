BEGIN
   ddls.prc_create_entity ('stag_queue'
                                         , 'stag_queue_code VARCHAR2(10),
                                            stag_queue_name VARCHAR2(100)'
                                         , 'DROP'
                                         , TRUE
                                         , TRUE
                                          );
END;
/

ALTER TABLE stag_queue_t ADD CONSTRAINT stag_queue_uk UNIQUE (stag_queue_code);

COMMENT ON TABLE stag_queue_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';