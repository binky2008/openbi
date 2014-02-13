package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestSchemaCopyFromDB2 {
	
	private String[] arguments = new String[26];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2] = "-srcdbdriverclass";
		arguments[4] = "-srcdbconnectionurl";
		arguments[6] = "-srcdbusername";
		arguments[8] = "-srcdbpassword";
		arguments[10] = "-sourceschema";
		arguments[12] = "-trgdbdriverclass";
		arguments[14] = "-trgdbconnectionurl";
		arguments[16] = "-trgdbusername";
		arguments[18] = "-trgdbpassword";
		arguments[20] = "-targetschema";
		arguments[22] = "-trgcreate";
		arguments[24] = "-dropifexists";
		
	}
	
	public void initSourceDB2() {
		// Source properties
		arguments[3] = "com.ibm.db2.jcc.DB2Driver";
		arguments[5] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[7] = "db2user";
		arguments[9] = "db2user";
		arguments[11] = "sugarcrm";
	}

	/*@Test
	public void testDB2ToMySQL() {
		
		initArguments();
		initSourceDB2();
		//
		arguments[13] = "com.mysql.jdbc.Driver";
		arguments[15] = "jdbc:mysql://localhost:3306/sugarcrm_copy?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[17] = "sugarcrm";
		arguments[19] = "sugarcrm";
		arguments[21] = "sugarcrm_copy";
		arguments[23] = "true";
		arguments[25] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testDB2ToPostgreSQL() {
		
		initArguments();
		initSourceDB2();
		//
		arguments[13] = "org.postgresql.Driver";
		arguments[15] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "sugarcrm";
		arguments[23] = "true";
		arguments[25] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testDB2ToOracle() {
		
		initArguments();
		initSourceDB2();
		//
		arguments[13] = "oracle.jdbc.OracleDriver";
		arguments[15] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[17] = "sugarcrm_copy";
		arguments[19] = "sugarcrm_copy";
		arguments[21] = "sugarcrm_copy";
		arguments[23] = "true";
		arguments[25] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testDB2ToDB2() {
		
		initArguments();
		initSourceDB2();
		//
		arguments[13] = "com.ibm.db2.jcc.DB2Driver";
		arguments[15] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[17] = "db2user";
		arguments[19] = "db2user";
		arguments[21] = "sugarcrm_copy";
		arguments[23] = "true";
		arguments[25] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testDB2ToSQLServer() {
		
		initArguments();
		initSourceDB2();
		//
		arguments[13] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[15] = "jdbc:sqlserver://nikobook:1433;instance=SQLEXPRESS;database=dwhstage";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "sugarcrm";
		arguments[23] = "true";
		arguments[25] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e.getMessage() + "\n" + e.getStackTrace());
		}
	}*/
}