SET serveroutput ON;

BEGIN
   ddl.prc_create_entity
      ('stg_source'
     , 'stg_source_code VARCHAR2(10),
		stg_source_prefix VARCHAR2(10),
		stg_source_name VARCHAR2(1000),
		stg_owner VARCHAR2(100),
		stg_ts_stg1_data VARCHAR2(100),
		stg_ts_stg1_indx VARCHAR2(100),
		stg_ts_stg2_data VARCHAR2(100),
		stg_ts_stg2_indx VARCHAR2(100),
		stg_fb_archive VARCHAR2(100),
		stg_bodi_ds VARCHAR2(100),
		stg_source_bodi_ds VARCHAR2(100)'
     , 'DROP'
     , TRUE
     , TRUE
      );
END;
/

COMMENT ON TABLE stg_source_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';