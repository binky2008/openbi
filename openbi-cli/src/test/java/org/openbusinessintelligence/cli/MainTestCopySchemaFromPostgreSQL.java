package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.helper.MainTestCopySchemaHelper;

public class MainTestCopySchemaFromPostgreSQL {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_postgresql_postgres_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}
	
	@Test
	public void testPostgreSQLtoMySQL() {
		
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
	public void testPostgreSQLtoPostgreSQL() {
		
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
	public void testPostgreSQLtoOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_dwhstage";
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
	public void testPostgreSQLtoDB2() {
		
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
	public void testPostgreSQLtoInformix() {
		
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
	public void testPostgreSQLtoSQLServer() {
		
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
	public void testPostgreSQLtoHANA() {
		
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
	public void testPostgreSQLtoTeradata() {
		
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