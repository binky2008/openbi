SET serveroutput ON;
BEGIN
	ddl.prc_create_entity (
		'stg_stat_type'
	 , 'stg_stat_type_code VARCHAR2(10),
		 stg_stat_type_name VARCHAR2(100),
		 stg_stat_type_desc VARCHAR2(1000)'
	 , 'DROP'
	 , TRUE
     , TRUE) ;
END;
/

COMMENT ON TABLE stg_stat_type_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';