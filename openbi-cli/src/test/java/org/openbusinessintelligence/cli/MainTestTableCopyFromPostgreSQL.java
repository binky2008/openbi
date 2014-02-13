package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopyFromPostgreSQL {
	
	private String[] arguments = new String[20];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "-function";
		arguments[1]  = "tablecopy";
		// Mandatory arguments
		arguments[2]  = "-srcdbconnpropertyfile";
		arguments[4]  = "-srcdbconnkeywordfile";
		arguments[6]  = "-sourcetable";
		arguments[8]  = "-trgdbconnpropertyfile";
		arguments[10] = "-trgdbconnkeywordfile";
		arguments[12] = "-targetschema";
		arguments[14] = "-targettable";
		
		arguments[16] = "-trgcreate";
		arguments[17] = "true";
		arguments[18] = "-dropifexists";
		arguments[19] = "true";
		
	}
	
	public void initSourcePostgreSQL() {
		// Source properties
		arguments[3] = "postgresql_localhost_postgres_test";
		arguments[5] = "";
		arguments[7] = "tab_test";
	}

	@Test
	public void testPostgreSQLToMySQL() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[9] = "mysql_localhost_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_psg_tab_test";
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
		arguments[9]  = "postgresql_localhost_postgres_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_psg_tab_test";
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
		arguments[9]  = "oracle_localhost_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_psg_tab_test";
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
		arguments[9]  = "db2_localhost_sample_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_psg_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testPostgreSQLToInformix() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[9]  = "informix_localhost_test";
		arguments[11] = "";
		arguments[13] = "test";
		arguments[15] = "stg_psg_tab_test";
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
		arguments[9]  = "sqlserver_localhost_test";
		arguments[11] = "";
		arguments[13] = "dbo";
		arguments[15] = "stg_psg_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testPostgreSQLtoHANA() {
		
		initArguments();
		initSourcePostgreSQL();
		//
		arguments[9] = "hana_msas120i_01_dwh_stage";
		arguments[11] = "";
		arguments[13] = "dwh_stage";
		arguments[15] = "stg_psg_tab_test";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}