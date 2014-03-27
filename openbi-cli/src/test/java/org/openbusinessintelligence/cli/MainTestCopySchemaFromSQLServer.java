package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopySchemaFromSQLServer {
	
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
	
	public void initSourceSQLServer() {
		// Source properties
		arguments[3] = "localhost_sqlserver_sugarcrm";
		arguments[5] = "";
		arguments[7] = "sugarcrm";
	}

	@Test
	public void testSQLServerToMySQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9]  = "localhost_mysql_dwhstage";
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
	public void testSQLServerToPostgreSQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9]  = "localhost_postgresql_postgres_sugarcrm";
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
	public void testSQLServerToOracle() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9]  = "localhost_oracle_sugarcrm";
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
	public void testSQLServerToDB2() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9]  = "localhost_db2_sample_sugarcrm";
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
	public void testSQLServertoInformix() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9]  = "localhost_informix_sugarcrm";
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
	public void testSQLServertoSQLServer() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9] = "localhost_sqlserver_sugarcrm";
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
	public void testSQLServertoHANA() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9] = "msas120i_hana_01_sugarcrm";
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

	@Test
	public void testSQLServertoTeradata() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[9] = "localhost_teradata_sugarcrm";
		arguments[11] = "TDBKeywords";
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