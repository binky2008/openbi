SET serveroutput ON;

begin
   aux_ddl.prc_create_entity
            ('doc'
           , 'doc_type VARCHAR2 (100),
              doc_code VARCHAR2 (100),
              doc_url VARCHAR2 (4000),
              doc_desc VARCHAR2 (4000),
              doc_content CLOB'
           , 'DROP'
           , TRUE
           , TRUE
            );
END;
/

COMMENT ON TABLE doc_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';
