SET serveroutput ON;

BEGIN
   ddl.prc_create_entity ('txn_user'
                            , 'user_id NUMBER NOT NULL,
                               txn_id NUMBER NOT NULL'
                            , 'DROP'
                            , TRUE
                            , TRUE
                           );
END;
/

ALTER TABLE txn_user_t ADD (CONSTRAINT txn_user_uk UNIQUE (user_id,txn_id));

COMMENT ON TABLE txn_user_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';