package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopyTableFromOracle {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[4];
	
	private void initSource() {
		sourceArgs[0] = "localhost_oracle_test";
		sourceArgs[1] = "";
		sourceArgs[2] = "tab_test";
	}

	@Test
	public void testOracleToMySQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_mysql_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}


	@Test
	public void testOracleToPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_sample_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_test";
		targetArgs[1] = "";
		targetArgs[2] = "dbo";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracleToHANA() {
		
		initSource();
		//
		targetArgs[0] = "msas120i_hana_01_dwh_stage";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "dwh_stage";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
	
	@Test
	public void testOracleToTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_test";
		targetArgs[1] = "TDBKeywords";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_ora_tab_test";
		//
		MainTestCopyTableHelper.initSource(sourceArgs);
		MainTestCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}