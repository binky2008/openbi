-- Test
DROP USER test;

CREATE USER test FROM DBC AS 
	PASSWORD  =  test,
	PERMANENT = 10485760;
	
grant select on DBC.UDTInfo to test;	

-- SugarCRM
DROP USER sugarcrm;

CREATE USER sugarcrm FROM DBC AS 
	PASSWORD  =  sugarcrm,
	PERMANENT = 209715200;

grant select on DBC.UDTInfo to sugarcrm;

-- DWHSTAGE
DROP USER dwhstage;

CREATE USER dwhstage FROM DBC AS 
	PASSWORD  =  dwhstage,
	PERMANENT = 209715200;

grant select on DBC.UDTInfo to dwhstage;