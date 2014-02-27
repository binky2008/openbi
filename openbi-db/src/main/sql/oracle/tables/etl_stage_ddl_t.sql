SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
            ('etl_stage_ddl'
           , 'etl_stage_ddl_type VARCHAR2 (100),
              etl_stage_ddl_name VARCHAR2 (100),
              etl_stage_ddl_code CLOB'
           , 'DROP'
           , TRUE
           , TRUE
            );
END;
/

COMMENT ON TABLE etl_stage_ddl_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';