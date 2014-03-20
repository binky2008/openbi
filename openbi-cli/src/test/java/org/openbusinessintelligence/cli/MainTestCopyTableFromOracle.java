package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopyTableFromOracle {
	
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
	
	public void initSourceOracle() {
		// Source properties
		arguments[3] = "localhost_oracle_test";
		arguments[5] = "";
		arguments[7] = "tab_test";
	}

	@Test
	public void testOracleToMySQL() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[9] = "localhost_mysql_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}


	@Test
	public void testOracleToPostgreSQL() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[9]  = "localhost_postgresql_postgres_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToOracle() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[9] = "localhost_oracle_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToDB2() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[9]  = "localhost_db2_sample_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
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
		initSourceOracle();
		//
		arguments[9]  = "localhost_informix_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToSQLServer() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[9]  = "localhost_sqlserver_test";
		arguments[11] = "";
		arguments[13] = "dbo";
		arguments[15] = "stg_ora_tab_test";
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
		initSourceOracle();
		//
		arguments[9] = "msas120i_hana_01_dwh_stage";
		arguments[11] = "HDBKeywords";
		arguments[13] = "dwh_stage";
		arguments[15] = "stg_ora_tab_test";
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
		initSourceOracle();
		//
		arguments[9] = "localhost_teradata_test";
		arguments[11] = "TDBKeywords";
		arguments[13] = "test";
		arguments[15] = "stg_ora_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}