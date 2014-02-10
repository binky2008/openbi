package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopyFromSQLServer {
	
	private String[] arguments = new String[30];
	
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
		arguments[12] = "-sourcetable";
		arguments[14] = "-trgdbdriverclass";
		arguments[16] = "-trgdbconnectionurl";
		arguments[18] = "-trgdbusername";
		arguments[20] = "-trgdbpassword";
		arguments[22] = "-targetschema";
		arguments[24] = "-targettable";
		arguments[26] = "-trgcreate";
		arguments[28] = "-dropifexists";
		
	}
	
	public void initSourceSQLServer() {
		// Source properties
		arguments[3] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[5] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;databaseName=AdventureWorks2012";
		arguments[7] = "dwhadmin";
		arguments[9] = "openbi";
		arguments[11] = "Sales";
		arguments[13] = "SalesOrderDetail";
	}

	@Test
	public void testSQLServerToMySQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[15] = "com.mysql.jdbc.Driver";
		arguments[17] = "jdbc:mysql://localhost:3306/dwhstage?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_salesorderdetail";
		arguments[27] = "true";
		arguments[29] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}


	@Test
	public void testSQLServerToPostgreSQL() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[15] = "org.postgresql.Driver";
		arguments[17] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[19] = "dwhload";
		arguments[21] = "dwhload";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_salesorderdetail";
		arguments[27] = "true";
		arguments[29] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLServerToOracle() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[15] = "oracle.jdbc.OracleDriver";
		arguments[17] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_salesorderdetail";
		arguments[27] = "true";
		arguments[29] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLServerToDB2() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[15] = "com.ibm.db2.jcc.DB2Driver";
		arguments[17] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[19] = "db2user";
		arguments[21] = "db2user";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_salesorderdetail";
		arguments[27] = "true";
		arguments[29] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLServerToSQLServer() {
		
		initArguments();
		initSourceSQLServer();
		//
		arguments[15] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[17] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;database=dwhstage";
		arguments[19] = "dwhload";
		arguments[21] = "dwhload";
		arguments[23] = "dbo";
		arguments[25] = "stg_salesorderdetail";
		arguments[27] = "true";
		arguments[29] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}