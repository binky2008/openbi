SET SERVEROUTPUT ON;

BEGIN
   ddls.prc_create_entity (
      'mesr_taxn',
      'mesr_query_id NUMBER NOT NULL,
	   taxn_id NUMBER NOT NULL',
      'DROP',
      TRUE,
      TRUE);
END;
/

CREATE UNIQUE INDEX mesr_taxn_uk
   ON mesr_taxn_t (mesr_query_id, taxn_id);

COMMENT ON TABLE mesr_taxn_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';