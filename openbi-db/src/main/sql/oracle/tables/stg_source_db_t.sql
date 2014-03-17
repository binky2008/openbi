SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
      ('stg_source_db'
     , 'stg_source_id          NUMBER,
        stg_source_db_link     VARCHAR2(100),
        stg_source_db_jdbcname VARCHAR2(100),
        stg_source_owner       VARCHAR2(100),
        stg_distribution_code  VARCHAR2(10),
        stg_source_bodi_ds     VARCHAR2(100)'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE stg_source_db_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';