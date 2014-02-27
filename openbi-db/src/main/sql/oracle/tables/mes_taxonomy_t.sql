SET SERVEROUTPUT ON;

BEGIN
   utl_ddl.prc_create_entity (
      'mes_taxonomy',
      'mes_query_id NUMBER NOT NULL,
	     txn_id NUMBER NOT NULL',
      'DROP',
      TRUE,
      TRUE);
END;
/

CREATE UNIQUE INDEX mes_taxonomy_uk
   ON mes_taxonomy_t (mes_query_id, txn_taxonomy_id);

COMMENT ON TABLE mes_taxonomy_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';