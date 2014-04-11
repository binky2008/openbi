package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopySchemaFromDB2 {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_db2_sample_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}

	@Test
	public void testDB2ToMySQL() {
		
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
	public void testDB2ToPostgreSQL() {
		
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
	public void testDB2ToOracle() {
		
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
	public void testDB2ToDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_sample_dwhstage";
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
	public void testDB2ToInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_sugarcrm";
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
	public void testDB2ToSQLServer() {
		
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
	public void testDB2ToHANA() {
		
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
	public void testDB2ToTeradata() {
		
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