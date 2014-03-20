package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestETLXMLSchema {
	
	private String[] arguments = new String[20];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "-function";
		arguments[1]  = "createetlxml";
		// Mandatory arguments
		arguments[2]  = "-srcdbconnpropertyfile";
		arguments[4] = "-srcdbconnkeywordfile";
		arguments[6] = "-sourceschema";
		//
		arguments[8] = "-bodidataflowprefix";
		arguments[10] = "-bodiworkflowprefix";
		arguments[12] = "-bodijobprefix";
		arguments[14] = "-bodisourcedatastore";
		arguments[16] = "-boditargetdatastore";
		arguments[18] = "-bodiexportfile";
	}
	
	public void initSourceMySQL() {
		// Target properties
		arguments[3] = "msas4263ixl_mysql_sugarcrm";
		arguments[5] = "";
		arguments[7] = "sugarcrm";
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9] = "df";
		arguments[11] = "wf";
		arguments[13] = "jb";
		arguments[15] = "msas4263ixl_mysql_sugarcrm";
		arguments[17] = "msas4263ixl_oracle_dwhstage";
		arguments[19] = "D:/DEV/test.xml";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	/*@Test
	public void testMySQLtoPostgreSQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "localhost_postgresql_postgres_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoOracle() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9] = "localhost_oracle_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoDB2() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "localhost_db2_sample_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoInformix() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "localhost_informix_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoSQLServer() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "localhost_sqlserver_test";
		arguments[11] = "";
		arguments[13] = "dbo";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoHANA() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9] = "msas120i_hana_01_dwh_stage";
		arguments[11] = "HDBKeywords";
		arguments[13] = "dwh_stage";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoTeradata() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9] = "localhost_teradata_test";
		arguments[11] = "TDBKeywords";
		arguments[13] = "test";
		arguments[15] = "stg_mys_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}*/
}