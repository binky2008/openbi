package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.helper.MainTestCopyTableHelper;

public class MainTestCopyTableFromPostgreSQL {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[4];
	
	private void initSource() {
		sourceArgs[0] = "localhost_postgresql_postgres_test";
		sourceArgs[1] = "";
		sourceArgs[2] = "tab_test";
	}

	@Test
	public void testPostgreSQLToMySQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_mysql_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLToPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLToOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLToDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_sample_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLToInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_test";
		targetArgs[1] = "";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLToSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_test";
		targetArgs[1] = "";
		targetArgs[2] = "dbo";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLtoHANA() {
		
		initSource();
		//
		targetArgs[0] = "msas120i_hana_01_dwh_stage";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "dwh_stage";
		targetArgs[3] = "stg_psg_tab_test";
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
	public void testPostgreSQLtoTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_test";
		targetArgs[1] = "TDBKeywords";
		targetArgs[2] = "test";
		targetArgs[3] = "stg_psg_tab_test";
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