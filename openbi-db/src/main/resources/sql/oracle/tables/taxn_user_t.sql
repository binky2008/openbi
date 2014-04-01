SET serveroutput ON;

BEGIN
   ddls.prc_create_entity ('taxn_user'
                            , 'user_id NUMBER NOT NULL,
                               taxn_id NUMBER NOT NULL'
                            , 'DROP'
                            , TRUE
                            , TRUE
                           );
END;
/

ALTER TABLE taxn_user_t ADD (CONSTRAINT taxn_user_uk UNIQUE (user_id,taxn_id));

COMMENT ON TABLE taxn_user_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';