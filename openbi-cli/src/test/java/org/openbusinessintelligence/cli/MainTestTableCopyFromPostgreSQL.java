package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopyFromPostgreSQL {
	
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
	
	public void initSourcePostgreSQL() {
		// Source properties
		arguments[3] = "org.postgresql.Driver";
		arguments[5] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[7] = "dwhload";
		arguments[9] = "dwhload";
		arguments[11] = "sugarcrm";
		arguments[13] = "campaigns";
	}

	@Test
	public void testPostgreSQLToMySQL() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[15] = "com.mysql.jdbc.Driver";
		arguments[17] = "jdbc:mysql://localhost:3306/dwhstage?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_campaigns";
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
	public void testPostgreSQLToPostgreSQL() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[15] = "org.postgresql.Driver";
		arguments[17] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[19] = "dwhload";
		arguments[21] = "dwhload";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_campaigns";
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
	public void testPostgreSQLToOracle() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[15] = "oracle.jdbc.OracleDriver";
		arguments[17] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_campaigns";
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
	public void testPostgreSQLToDB2() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[15] = "com.ibm.db2.jcc.DB2Driver";
		arguments[17] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[19] = "db2user";
		arguments[21] = "db2user";
		arguments[23] = "dwhstage";
		arguments[25] = "stg_campaigns";
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
	public void testPostgreSQLToSQLServer() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[15] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[17] = "jdbc:sqlserver://localhost:1433;instance=SQLEXPRESS;database=dwhstage";
		arguments[19] = "dwhload";
		arguments[21] = "dwhload";
		arguments[23] = "dbo";
		arguments[25] = "stg_campaigns";
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