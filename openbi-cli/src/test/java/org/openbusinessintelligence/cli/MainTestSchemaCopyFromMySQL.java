package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestSchemaCopyFromMySQL {
	
	private String[] arguments = new String[18];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2]  = "-srcdbconnpropertyfile";
		arguments[4]  = "-srcdbconnkeywordfile";
		arguments[6]  = "-sourceschema";
		arguments[8]  = "-trgdbconnpropertyfile";
		arguments[10] = "-trgdbconnkeywordfile";
		arguments[12] = "-targetschema";
		//
		arguments[14] = "-trgcreate";
		arguments[15] = "true";
		arguments[16] = "-dropifexists";
		arguments[17] = "true";
	}
	
	public void initSourceMySQL() {
		// Source properties
		arguments[3] = "mysql_localhost_sugarcrm";
		arguments[5] = "";
		arguments[7] = "sugarcrm";
	}

	@Test
	public void testMySQLtoMySQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "mysql_localhost_dwhstage";
		arguments[11] = "";
		arguments[13] = "dwhstage";
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
		arguments[9] = "postgresql_localhost_postgres_sugarcrm";
		arguments[11] = "";
		arguments[13] = "sugarcrm";
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
		arguments[9] = "oracle_localhost_sugarcrm";
		arguments[11] = "";
		arguments[13] = "sugarcrm";
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
		arguments[9] = "db2_localhost_sample_sugarcrm";
		arguments[11] = "";
		arguments[13] = "sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoInformix() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9]  = "informix_localhost_sugarcrm";
		arguments[11] = "";
		arguments[13] = "sugarcrm";
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
		arguments[9] = "sqlserver_localhost_sugarcrm";
		arguments[11] = "";
		arguments[13] = "dbo";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testMySQLtoHANA() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[9] = "hana_msas120i_01_sugarcrm";
		arguments[11] = "HDBKeywords";
		arguments[13] = "sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}
}