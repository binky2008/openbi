package org.openbusinessintelligence.cli.qb;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestQBCopySchemaFromMySQL {
	
	private String[] sourceArgs = new String[3];
	private String[] targetArgs = new String[3];
	
	private void initSource() {
		sourceArgs[0] = "qb/procus3_mysql_opst_105_proc";
		sourceArgs[1] = "";
		sourceArgs[2] = "opst_105_proc";
	}
	
	@Test
	public void testMySQLtoMySQL() {
		
		initSource();
		//
		targetArgs[0] = "qb/localhost_mysql_opst_105_proc";
		targetArgs[1] = "";
		targetArgs[2] = "opst_105_proc";
		//
		MainTestQBCopySchemaHelper.initSource(sourceArgs);
		MainTestQBCopySchemaHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestQBCopySchemaHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}