SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
            ('stg_ddl'
           , 'stg_ddl_type VARCHAR2 (100),
              stg_ddl_name VARCHAR2 (100),
              stg_ddl_code CLOB'
           , 'DROP'
           , TRUE
           , TRUE
            );
END;
/

COMMENT ON TABLE stg_ddl_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';