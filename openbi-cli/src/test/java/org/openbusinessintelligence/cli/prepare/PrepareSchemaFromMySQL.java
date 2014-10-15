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
	public void testDB2() {
		
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
	public void testDerby() {
		
		initSource();
		//
		targetArgs[0] = "localhost_derby_dwhdev_sugarcrm";
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
	public void testExasol() {
		
		initSource();
		//
		targetArgs[0] = "localhost_exasol_sugarcrm";
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
	public void testFirebird() {
		
		initSource();
		//
		targetArgs[0] = "localhost_firebird_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "";
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
	public void testH2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_h2_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "public";
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
	public void testHDB() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hana_01_sugarcrm";
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
	public void testHSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hsql_sugarcrm";
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
	public void testImpala() {
		
		initSource();
		//
		targetArgs[0] = "localhost_impala_sugarcrm";
		targetArgs[1] = "IMPALAKeywords";
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
	public void testInformix() {
		
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
	public void testNetezza() {
		
		initSource();
		//
		targetArgs[0] = "localhost_netezza_sugarcrm";
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
	public void testOracle() {
		
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
	public void testPostgreSQL() {
		
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
	public void testSQLAnywhere() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlanywhere_sugarcrm_sugarcrm";
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
	public void testSQLServer() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlserver_sugarcrm";
		targetArgs[1] = "";
		targetArgs[2] = "";
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
	public void testTeradata() {
		
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

	@Test
	public void testVertica() {
		
		initSource();
		//
		targetArgs[0] = "localhost_vertica_dwhdev_sugarcrm";
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
}