SET serveroutput ON;
BEGIN
	ddls.prc_create_entity (
		'stag_stat_type'
	 , 'stag_stat_type_code VARCHAR2(10),
		 stag_stat_type_name VARCHAR2(100),
		 stag_stat_type_desc VARCHAR2(1000)'
	 , 'DROP'
	 , TRUE
     , TRUE) ;
END;
/

COMMENT ON TABLE stag_stat_type_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';