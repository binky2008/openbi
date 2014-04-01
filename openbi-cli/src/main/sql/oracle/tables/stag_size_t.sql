SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
      ('stag_size'
     , 'stag_object_id NUMBER,
        stag_table_name VARCHAR2(100),
        stag_num_rows NUMBER,
        stag_bytes NUMBER'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

ALTER TABLE stag_size_t ADD (CONSTRAINT stag_size_uk UNIQUE (stag_table_name, create_date));

COMMENT ON TABLE stag_size_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';