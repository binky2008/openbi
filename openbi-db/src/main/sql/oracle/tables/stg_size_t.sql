SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
      ('stg_size'
     , 'stg_object_id NUMBER,
        stg_table_name VARCHAR2(100),
        stg_num_rows NUMBER,
        stg_bytes NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

ALTER TABLE stg_size_t ADD (CONSTRAINT stg_size_uk UNIQUE (stg_table_name, create_date));

COMMENT ON TABLE stg_size_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';