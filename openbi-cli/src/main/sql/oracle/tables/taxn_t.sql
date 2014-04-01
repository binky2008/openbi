SET serveroutput ON;

BEGIN
   ddls.prc_create_entity ('taxn'
                         , 'taxn_parent_id NUMBER,
							taxn_order NUMBER,
                            taxn_code VARCHAR2 (100),
                            taxn_name VARCHAR2 (4000)'
                         , 'DROP'
                         , TRUE
                         , TRUE
                         );
END;
/

ALTER TABLE taxn_t ADD (CONSTRAINT taxn_uk UNIQUE (taxn_code));

MERGE INTO taxn_t trg
   USING (SELECT 'GLOBAL' AS taxn_code
            FROM DUAL) src
   ON (trg.taxn_code = src.taxn_code)
   WHEN NOT MATCHED THEN
      INSERT (taxn_id, taxn_code, taxn_name)
      VALUES (-1, 'GLOBAL', 'Global');

COMMIT ;

COMMENT ON TABLE taxn_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';