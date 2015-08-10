package org.openbusinessintelligence.cli.qb;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MainTestQBCopyTableFromMySQL {
	
	private String[] sourceArgs = new String[4];
	private String[] targetArgs = new String[4];
	
	private void initSource() {
		sourceArgs[0] = "qb/procus3_mysql_opst_105_proc";
		sourceArgs[1] = "";
		sourceArgs[2] = "opst_105_proc";
		sourceArgs[3] = "survey_session";
	}

	@Test
	public void testMySQL() {
		
		initSource();
		//
		targetArgs[0] = "qb/localhost_mysql_opst_105_proc";
		targetArgs[1] = "";
		targetArgs[2] = "opst_105_proc";
		targetArgs[3] = "survey_session";
		//
		MainTestQBCopyTableHelper.initSource(sourceArgs);
		MainTestQBCopyTableHelper.initTarget(targetArgs);
		// Perform test
		try {
			MainTestQBCopyTableHelper.execute();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}