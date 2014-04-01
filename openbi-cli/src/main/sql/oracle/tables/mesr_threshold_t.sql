SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
         ('mesr_threshold'
        , 'mesr_keyfigure_id NUMBER,
           mesr_threshold_type CHAR(1) DEFAULT ''A'' CHECK (mesr_threshold_type IN (''A'',''I'')),
           mesr_threshold_from DATE,
           mesr_threshold_to DATE,
           mesr_threshold_min FLOAT,
           mesr_threshold_max FLOAT'
         , 'DROP'
	     , TRUE
         , TRUE
         );
END;
/

CREATE UNIQUE INDEX mesr_threshold_uk ON mesr_threshold_t (mesr_keyfigure_id,mesr_threshold_from);

COMMENT ON TABLE mesr_threshold_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';