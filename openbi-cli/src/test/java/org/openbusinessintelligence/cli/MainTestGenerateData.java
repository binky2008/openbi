package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestGenerateData {
	
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

	@Test
	public void testDB2() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_db2_dwhdev_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testDerby() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_derby_dwhdev";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testExasol() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_exasol_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testFirebird() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_firebird_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testH2() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_h2_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testHDB() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_hana_01_dwh_test";
		arguments[4] = "";
		arguments[6] = "dwh_test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testHSQL() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_hsql_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testImpala() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_impala_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformix() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_informix_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_mysql_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testNetezza() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_netezza_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracle() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_oracle_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testPostgreSQL() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_postgresql_postgres_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLAnywhere() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_sybase_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLServer() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_sqlserver_test";
		arguments[4] = "";
		arguments[6] = "dbo";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testTeradata() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_teradata_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testVertica() {
		
		initArguments();
		
		// Target properties
		arguments[2] = "localhost_vertica_dwhdev_test";
		arguments[4] = "";
		arguments[6] = "";
		arguments[8] = "tab_test";
		arguments[10] = "10";
		
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}