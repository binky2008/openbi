package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopyTableFromInformix {
	
	private String[] arguments = new String[20];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "-function";
		arguments[1]  = "tablecopy";
		// Mandatory arguments
		arguments[2]  = "-srcdbconnpropertyfile";
		arguments[4]  = "-srcdbconnkeywordfile";
		arguments[6]  = "-sourcetable";
		arguments[8]  = "-trgdbconnpropertyfile";
		arguments[10] = "-trgdbconnkeywordfile";
		arguments[12] = "-targetschema";
		arguments[14] = "-targettable";
		
		arguments[16] = "-trgcreate";
		arguments[17] = "true";
		arguments[18] = "-dropifexists";
		arguments[19] = "true";
		
	}
	
	public void initSourceInformix() {
		// Source properties
		arguments[3] = "localhost_informix_test";
		arguments[5] = "";
		arguments[7] = "test.tab_test";
	}

	@Test
	public void testInformixToMySQL() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9] = "localhost_mysql_dwhstage";
		arguments[11] = "";
		arguments[13] = "dwhstage";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToPostgreSQL() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9]  = "localhost_postgresql_postgres_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToOracle() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9] = "localhost_oracle_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToDB2() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9]  = "localhost_db2_sample_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToInformix() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9]  = "localhost_informix_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToSQLServer() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9]  = "localhost_sqlserver_test";
		arguments[11] = "";
		arguments[13] = "dbo";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	
	@Test
	public void testInformixToHANA() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9] = "msas120i_hana_01_dwh_stage";
		arguments[11] = "HDBKeywords";
		arguments[13] = "dwh_stage";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testInformixToTeradata() {
		
		initArguments();
		initSourceInformix();
		//
		arguments[9] = "localhost_teradata_test";
		arguments[11] = "TDBKeywords";
		arguments[13] = "test";
		arguments[15] = "stg_ifx_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}