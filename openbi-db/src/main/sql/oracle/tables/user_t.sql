SET serveroutput ON;

BEGIN
    
   ddl.prc_create_entity
                 ('user'
                , 'user_code VARCHAR2 (10),
                   user_name VARCHAR2 (100),
                   user_email VARCHAR2 (1000)'
                , 'DROP'
                , TRUE
                , true
                , TRUE
                 );
END;
/

ALTER TABLE user_t ADD (CONSTRAINT user_uk UNIQUE (user_code));

COMMENT ON TABLE user_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';
