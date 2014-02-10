package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestSchemaCopyFromMySQL {
	
	private String[] arguments = new String[14];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2] = "-srcdbconnpropertyfile";
		arguments[4] = "-sourceschema";
		arguments[6] = "-trgdbconnpropertyfile";
		arguments[8] = "-targetschema";
		//
		arguments[10] = "-trgcreate";
		arguments[11] = "true";
		arguments[12] = "-dropifexists";
		arguments[13] = "true";
	}
	
	public void initSourceMySQL() {
		// Source properties
		arguments[3] = "mysql_localhost_sugarcrm";
		arguments[5] = "sugarcrm";
	}

	@Test
	public void testMySQLtoMySQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "mysql_localhost_dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoPostgreSQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "postgresql_localhost_postgres_dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoOracle() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "oracle_localhost_dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoDB2() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "db2_localhost_dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoSQLServer() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "sqlserver_localhost_dwh";
		arguments[9] = "stage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}
}