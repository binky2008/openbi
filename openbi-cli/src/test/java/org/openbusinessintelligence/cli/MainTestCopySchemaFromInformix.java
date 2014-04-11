package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopySchemaFromInformix {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_informix_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}

	@Test
	public void testInformixToMySQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_mysql_dwhstage";
		targetArgs[1] = "";
		targetArgs[2] = "dwhstage";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_sample_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_dwhstage";
		targetArgs[1] = "";
		targetArgs[2] = "dwhstage";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "dbo";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToHANA() {
		
		initSource();
		//
		targetArgs[0] = "msas120i_hana_01_sugarcrm";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "sugarcrm";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testInformixToTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_sugarcrm";
		targetArgs[1] = "TDBKeywords";
		targetArgs[2] = "sugarcrm";
		//
		MainTestCopySchemaHelper.initSource(sourceArgs);
		MainTestCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}