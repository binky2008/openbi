package org.openbusinessintelligence.cli.copy.table;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestCopyTableFromHSQL {
	private String[] sourceArgs = new String[4];
	private String[] targetArgs = new String[4];
	
	private void initSource() {
		sourceArgs[0] = "localhost_hsql_test";
		sourceArgs[1] = "";
		sourceArgs[2] = "test";
		sourceArgs[3] = "tab_test";
	}

	@Test
	public void testDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_dwhdev_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testDerby() {
		
		initSource();
		//
		targetArgs[0] = "localhost_derby_dwhdev";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testFirebird() {
		
		initSource();
		//
		targetArgs[0] = "localhost_firebird_test";
		targetArgs[1] = "";
		targetArgs[2] = "";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testH2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_h2_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testHDB() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hana_01_dwh_test";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "dwh_test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testHSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hsql_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testMySQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_mysql_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testNetezza() {
		
		initSource();
		//
		targetArgs[0] = "localhost_netezza_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testSQLAnywhere() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sybase_test";
		targetArgs[1] = "";
		targetArgs[2] = "";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_test";
		targetArgs[1] = "";
		targetArgs[2] = "dbo";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_test";
		targetArgs[1] = "TDBKeywords";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_hsql_tab_test";
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
	public void testVertica() {
		
		initSource();
		//
		targetArgs[0] = "localhost_vertica_dwhdev_test";
		targetArgs[1] = "";
		targetArgs[2] = "";
		targetArgs[3] = "stg_hsql_tab_test";
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