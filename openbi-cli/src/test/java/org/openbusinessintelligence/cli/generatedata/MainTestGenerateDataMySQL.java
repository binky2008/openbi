package org.openbusinessintelligence.cli.generatedata;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestGenerateDataMySQL {
	
	private String[] arguments = new String[11];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "generaterandomdata";
		// Mandatory arguments
		arguments[1]  = "-trgdbconnpropertyfile";
		arguments[3] = "-trgdbconnkeywordfile";
		arguments[5] = "-targetschema";
		arguments[7] = "-targettable";
		//
		arguments[9] = "-numberofrows";
	}
	
	public void initSource() {
		// Target properties
		arguments[2] = "localhost_mysql_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "1000";
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		initSource();
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