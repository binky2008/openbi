SET SERVEROUTPUT ON;

BEGIN
   ddl.prc_create_entity (
      'mes_txn',
      'mes_query_id NUMBER NOT NULL,
	   txn_id NUMBER NOT NULL',
      'DROP',
      TRUE,
      TRUE);
END;
/

CREATE UNIQUE INDEX mes_txn_uk
   ON mes_txn_t (mes_query_id, txn_id);

COMMENT ON TABLE mes_txn_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';