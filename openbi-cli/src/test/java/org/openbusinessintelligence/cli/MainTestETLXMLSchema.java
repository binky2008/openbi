package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestETLXMLSchema {
	
	private String[] arguments = new String[19];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "createetlxml";
		// Mandatory arguments
		arguments[1]  = "-srcdbconnpropertyfile";
		arguments[3] = "-srcdbconnkeywordfile";
		arguments[5] = "-sourceschema";
		//
		arguments[7] = "-bodidataflowprefix";
		arguments[9] = "-bodiworkflowprefix";
		arguments[11] = "-bodijobprefix";
		arguments[13] = "-bodisourcedatastore";
		arguments[15] = "-boditargetdatastore";
		arguments[17] = "-bodiexportfile";
	}
	
	public void initSourceMySQL() {
		// Target properties
		arguments[2] = "msas4263ixl_mysql_sugarcrm";
		arguments[4] = "";
		arguments[6] = "sugarcrm";
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[8] = "df";
		arguments[10] = "wf";
		arguments[12] = "jb";
		arguments[14] = "msas4263ixl_mysql_sugarcrm";
		arguments[16] = "msas4263ixl_oracle_dwhstage";
		arguments[18] = "D:/DEV/test.xml";
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
		arguments[9] = "localhost_oracle_dwhdev_test";
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