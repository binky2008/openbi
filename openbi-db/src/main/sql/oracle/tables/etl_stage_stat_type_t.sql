SET serveroutput ON;
BEGIN
	utl_ddl.prc_create_object_standard (
		'etl_stage_stat_type'
	 , 'etl_stage_stat_type_code VARCHAR2(10),
		 etl_stage_stat_type_name VARCHAR2(100),
		 etl_stage_stat_type_desc VARCHAR2(1000)'
	 , 'DROP'
	 , TRUE
     , TRUE) ;
END;
/

COMMENT ON TABLE etl_stage_stat_type_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';