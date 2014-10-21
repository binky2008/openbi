package org.openbusinessintelligence.cli.copy.schema;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopySchemaFromMySQL {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_mysql_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}
	
	@Test
	public void testMySQLtoMySQL() {
		
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
	public void testMySQLtoPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_dwhstage";
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
	public void testMySQLtoOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_dwhdev_dwhstage";
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
	public void testMySQLtoDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_dwhdev_dwhstage";
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
	public void testMySQLtoInformix() {
		
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
	public void testMySQLtoSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_dwh";
		targetArgs[1] = "";
		targetArgs[2] = "stage";
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
	public void testMySQLtoHANA() {
		
		initSource();
		//
		targetArgs[0] = "msas120i_hana_01_dwh_stage";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "dwh_stage";
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
	public void testMySQLtoTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_dwhstage";
		targetArgs[1] = "TDBKeywords";
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
}