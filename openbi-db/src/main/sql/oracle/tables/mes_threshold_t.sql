SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_entity
         ('mes_threshold'
        , 'mes_keyfigure_id NUMBER,
           mes_threshold_type CHAR(1) DEFAULT ''A'' CHECK (mes_threshold_type IN (''A'',''I'')),
           mes_threshold_from DATE,
           mes_threshold_to DATE,
           mes_threshold_min FLOAT,
           mes_threshold_max FLOAT'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mes_threshold_uk ON mes_threshold_t (mes_keyfigure_id,mes_threshold_from);

COMMENT ON TABLE mes_threshold_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';