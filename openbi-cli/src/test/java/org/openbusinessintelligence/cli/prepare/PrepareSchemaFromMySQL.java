package org.openbusinessintelligence.cli.prepare;

import static org.junit.Assert.*;

import org.junit.Test;

public class PrepareSchemaFromMySQL {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_mysql_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}

	@Test
	public void testMySQLtoPostgreSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_postgresql_postgres_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoOracle() {
		
		initSource();
		//
		targetArgs[0] = "localhost_oracle_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoDB2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_db2_dwhdev_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoInformix() {
		
		initSource();
		//
		targetArgs[0] = "localhost_informix_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "dbo";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoHANA() {
		
		initSource();
		//
		targetArgs[0] = "msas120i_hana_01_sugarcrm";
		targetArgs[1] = "HDBKeywords";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testMySQLtoTeradata() {
		
		initSource();
		//
		targetArgs[0] = "localhost_teradata_sugarcrm";
		targetArgs[1] = "TDBKeywords";
		targetArgs[2] = "sugarcrm";
		//
		PrepareSchemaHelper.initSource(sourceArgs);
		PrepareSchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			PrepareSchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}