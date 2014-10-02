package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestDBProperties {
	
	private String[] arguments = new String[5];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "dbproperties";
		// Mandatory arguments
		arguments[1] = "-dbconnpropertyfile";
		// Optional arguments
		arguments[3] = "-dbconnkeywordfile";
		
	}

	@Test
	public void testDB2() {
		
		initArguments();
		//
		arguments[2] = "localhost_db2_dwhdev_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_derby_dwhdev";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_exasol_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_firebird_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_h2_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_hana_01_dwh_stage";
		arguments[4] = "HDBKeywords";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testHive() {
		
		initArguments();
		//
		arguments[2] = "localhost_hive_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_hsql_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_impala_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testInfobright() {
		
		initArguments();
		//
		arguments[2] = "localhost_infobright_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_informix_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testIQ() {
		
		initArguments();
		//
		arguments[2] = "localhost_iq_test_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMariaDB() {
		
		initArguments();
		//
		arguments[2] = "localhost_mariadb_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMaxDB() {
		
		initArguments();
		//
		arguments[2] = "localhost_maxdb_maxdb_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMimer() {
		
		initArguments();
		//
		arguments[2] = "localhost_mimer_test_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	/*@Test
	public void testMonetDB() {
		
		initArguments();
		//
		arguments[2] = "localhost_monetdb_demo";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}*/

	@Test
	public void testMySQL() {
		
		initArguments();
		//
		arguments[2] = "localhost_mysql_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_netezza_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testNuoDB() {
		
		initArguments();
		//
		arguments[2] = "localhost_nuodb_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_oracle_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_postgresql_postgres_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_sqlanywhere_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_sqlserver_test";
		arguments[4] = "";
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
		arguments[2] = "localhost_teradata_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testVector() {
		
		initArguments();
		//
		arguments[2] = "localhost_vector_test";
		arguments[4] = "";
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
		//
		arguments[2] = "localhost_vertica_dwhdev_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
