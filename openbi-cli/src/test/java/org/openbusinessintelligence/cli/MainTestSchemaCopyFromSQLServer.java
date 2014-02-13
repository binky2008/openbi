package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestSchemaCopyFromSQLServer {
	
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
	
	public void initSourceSQLServer() {
		// Source properties
		arguments[3] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[5] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;databaseName=AdventureWorks2012";
		arguments[7] = "dwhadmin";
		arguments[9] = "openbi";
		arguments[11] = "Sales";
	}

	/*@Test
	public void testSQLServerToMySQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[13] = "com.mysql.jdbc.Driver";
		arguments[15] = "jdbc:mysql://localhost:3306/adventureworks?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[17] = "adventureworks";
		arguments[19] = "adventureworks";
		arguments[21] = "adventureworks";
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
	public void testSQLServerToPostgreSQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[13] = "org.postgresql.Driver";
		arguments[15] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "adventureworks";
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
	public void testSQLServerToOracle() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[13] = "oracle.jdbc.OracleDriver";
		arguments[15] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[17] = "adventureworks";
		arguments[19] = "adventureworks";
		arguments[21] = "adventureworks";
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
	public void testSQLServerToDB2() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[13] = "com.ibm.db2.jcc.DB2Driver";
		arguments[15] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[17] = "db2user";
		arguments[19] = "db2user";
		arguments[21] = "adventureworks";
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
	public void testSQLServertoSQLServer() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[13] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[15] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;database=dwhstage";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "adventureworks";
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