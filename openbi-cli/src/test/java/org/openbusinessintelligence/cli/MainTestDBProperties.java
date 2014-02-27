package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBProperties {
	
	private String[] arguments = new String[6];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "dbproperties";
		// Mandatory arguments
		arguments[2] = "-dbconnpropertyfile";
		// Optional arguments
		arguments[4] = "-dbconnkeywordfile";
		
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		//
		arguments[3] = "localhost_mysql_test";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_postgresql_postgres_test";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_sqlserver_test";
		arguments[5] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testDB2() {
		
		initArguments();
		//
		arguments[3] = "localhost_db2_sample_test";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_derby_sample";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_oracle_test";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_informix_test";
		arguments[5] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testHANA() {
		
		initArguments();
		//
		arguments[3] = "msas120i_hana_01_dwh_stage";
		arguments[5] = "HDBKeywords";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSybase() {
		
		initArguments();
		//
		arguments[3] = "localhost_sybase_demo";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_teradata_test";
		arguments[5] = "";
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
		//
		arguments[3] = "localhost_firebird_test";
		arguments[5] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
