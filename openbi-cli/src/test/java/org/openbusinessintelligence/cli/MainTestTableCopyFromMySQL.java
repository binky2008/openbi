package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopyFromMySQL {
	
	private String[] arguments = new String[16];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2] = "-srcdbconnpropertyfile";
		arguments[4] = "-sourcetable";
		arguments[6] = "-trgdbconnpropertyfile";
		arguments[8] = "-targetschema";
		arguments[10] = "-targettable";
		
		arguments[12] = "-trgcreate";
		arguments[13] = "true";
		arguments[14] = "-dropifexists";
		arguments[15] = "true";
		
	}
	
	public void initSourceMySQL() {
		// Source properties
		arguments[3] = "mysql_localhost_sugarcrm";
		arguments[5] = "campaigns";
	}

	@Test
	public void testMySQLtoMySQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "mysql_localhost_dwhstage";
		arguments[9] = "dwhstage";
		arguments[11] = "stg_campaigns";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}


	@Test
	public void testMySQLtoPostgreSQL() {
		
		initArguments();
		initSourceMySQL();
		//
		arguments[7] = "postgresql_localhost_postgres_dwhstage";
		arguments[9] = "dwhstage";
		arguments[11] = "stg_campaigns";
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
		arguments[7] = "oracle_localhost_dwhstage";
		arguments[9] = "dwhstage";
		arguments[11] = "stg_campaigns";
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
		arguments[7] = "db2_localhost_dwhstage";
		arguments[9] = "dwhstage";
		arguments[11] = "stg_campaigns";
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
		arguments[7] = "sqlserver_localhost_dwh";
		arguments[9] = "stage";
		arguments[11] = "stg_campaigns";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}