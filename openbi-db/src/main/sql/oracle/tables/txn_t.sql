SET serveroutput ON;

BEGIN
   ddl.prc_create_entity ('txn'
                         , 'txn_parent_id NUMBER,
							txn_order NUMBER,
                            txn_code VARCHAR2 (100),
                            txn_name VARCHAR2 (4000)'
                         , 'DROP'
                         , TRUE
                         , TRUE
                         );
END;
/

ALTER TABLE txn_t ADD (CONSTRAINT txn_uk UNIQUE (txn_code));

MERGE INTO txn_t trg
   USING (SELECT 'GLOBAL' AS txn_code
            FROM DUAL) src
   ON (trg.txn_code = src.txn_code)
   WHEN NOT MATCHED THEN
      INSERT (txn_id, txn_code, txn_name)
      VALUES (-1, 'GLOBAL', 'Global');

COMMIT ;

COMMENT ON TABLE txn_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';