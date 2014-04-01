SET serveroutput ON;

BEGIN
   ddls.prc_create_entity
      ('stag_source'
     , 'stag_source_code VARCHAR2(10),
		stag_source_prefix VARCHAR2(10),
		stag_source_name VARCHAR2(1000),
		stag_owner VARCHAR2(100),
		stag_ts_stg1_data VARCHAR2(100),
		stag_ts_stg1_indx VARCHAR2(100),
		stag_ts_stg2_data VARCHAR2(100),
		stag_ts_stg2_indx VARCHAR2(100),
		stag_fb_archive VARCHAR2(100),
		stag_bodi_ds VARCHAR2(100),
		stag_source_bodi_ds VARCHAR2(100)'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE stag_source_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';