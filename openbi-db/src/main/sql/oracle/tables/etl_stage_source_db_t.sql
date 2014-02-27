SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
      ('etl_stage_source_db'
     , 'etl_stage_source_id          NUMBER,
        etl_stage_source_db_link     VARCHAR2(100),
        etl_stage_source_db_jdbcname VARCHAR2(100),
        etl_stage_source_owner       VARCHAR2(100),
        etl_stage_distribution_code  VARCHAR2(10),
        etl_stage_source_bodi_ds     VARCHAR2(100)'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE etl_stage_source_db_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';