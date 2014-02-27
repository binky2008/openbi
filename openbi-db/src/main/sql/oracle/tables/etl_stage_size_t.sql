SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard
      ('etl_stage_size'
     , 'etl_stage_object_id NUMBER,
        etl_stage_table_name VARCHAR2(100),
        etl_stage_num_rows NUMBER,
        etl_stage_bytes NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

ALTER TABLE etl_stage_size_t ADD (CONSTRAINT etl_stage_size_uk UNIQUE (etl_stage_table_name, create_date));

COMMENT ON TABLE etl_stage_size_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';