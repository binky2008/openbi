package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopyFromOracle {
	
	private String[] arguments = new String[28];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2] = "-srcdbdriverclass";
		arguments[4] = "-srcdbconnectionurl";
		arguments[6] = "-srcdbusername";
		arguments[8] = "-srcdbpassword";
		arguments[10] = "-sourcetable";
		arguments[12] = "-trgdbdriverclass";
		arguments[14] = "-trgdbconnectionurl";
		arguments[16] = "-trgdbusername";
		arguments[18] = "-trgdbpassword";
		arguments[20] = "-targetschema";
		arguments[22] = "-targettable";
		arguments[24] = "-trgcreate";
		arguments[26] = "-dropifexists";
		
	}
	
	public void initSourceOracle() {
		// Source properties
		arguments[3] = "oracle.jdbc.OracleDriver";
		arguments[5] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[7] = "sugarcrm";
		arguments[9] = "sugarcrm";
		arguments[11] = "campaigns";
	}

	@Test
	public void testOracleToMySQL() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[13] = "com.mysql.jdbc.Driver";
		arguments[15] = "jdbc:mysql://localhost:3306/dwhstage?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[17] = "dwhstage";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "stg_campaigns";
		arguments[25] = "true";
		arguments[27] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}


	@Test
	public void testOracleToPostgreSQL() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[13] = "org.postgresql.Driver";
		arguments[15] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "dwhstage";
		arguments[23] = "stg_campaigns";
		arguments[25] = "true";
		arguments[27] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToOracle() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[13] = "oracle.jdbc.OracleDriver";
		arguments[15] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[17] = "dwhstage";
		arguments[19] = "dwhstage";
		arguments[21] = "dwhstage";
		arguments[23] = "stg_campaigns";
		arguments[25] = "true";
		arguments[27] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToDB2() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[13] = "com.ibm.db2.jcc.DB2Driver";
		arguments[15] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[17] = "db2user";
		arguments[19] = "db2user";
		arguments[21] = "dwhstage";
		arguments[23] = "stg_campaigns";
		arguments[25] = "true";
		arguments[27] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToSQLServer() {
		
		initArguments();
		initSourceOracle();
		//
		arguments[13] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[15] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER;database=dwhstage";
		arguments[17] = "dwhload";
		arguments[19] = "dwhload";
		arguments[21] = "dbo";
		arguments[23] = "stg_campaigns";
		arguments[25] = "true";
		arguments[27] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}