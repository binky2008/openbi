package org.openbusinessintelligence.cli.copy.schema;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.copy.schema.MainTestCopySchemaHelper;

public class MainTestCopySchemaFromDB2 {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "localhost_db2_dwhdev_sugarcrm";
		sourceArgs[1] = "";
		sourceArgs[2] = "sugarcrm";
	}

	@Test
	public void testDB2() {
		
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
	public void testDerby() {
		
		initSource();
		//
		targetArgs[0] = "localhost_derby_dwhdev_dwhstage";
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
	public void testExasol() {
		
		initSource();
		//
		targetArgs[0] = "localhost_exasol_dwhstage";
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
	public void testFirebird() {
		
		initSource();
		//
		targetArgs[0] = "localhost_firebird_dwhdev_dwhstage";
		targetArgs[1] = "";
		targetArgs[2] = "";
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
	public void testH2() {
		
		initSource();
		//
		targetArgs[0] = "localhost_h2_dev";
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
	public void testHDB() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hana_01_dwh_stage";
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
	public void testHSQL() {
		
		initSource();
		//
		targetArgs[0] = "localhost_hsql_dwhdev";
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
	public void testImpala() {
		
		initSource();
		//
		targetArgs[0] = "localhost_impala_dwhstage";
		targetArgs[1] = "IMPALAKeywords";
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
	public void testInformix() {
		
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
	public void testMySQL() {
		
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
	public void testNetezza() {
		
		initSource();
		//
		targetArgs[0] = "localhost_netezza_testdb_dwhstage";
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
	public void testOracle() {
		
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
	public void testPostgreSQL() {
		
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
	public void testSQLAnywhere() {
		
		initSource();
		//
		targetArgs[0] = "localhost_sqlanywhere_dwhdev_dwhstage";
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
	public void testSQLServer() {
		
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
	public void testTeradata() {
		
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

	@Test
	public void testVertica() {
		
		initSource();
		//
		targetArgs[0] = "localhost_vertica_dwhdev_dwhstage";
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
}