package org.openbusinessintelligence.cli;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.openbusinessintelligence.cli.copy.schema.MainTestCopySchemaHelper;

public class MainTestInstallFrameworkOracle {
	
	private String[] arguments = new String[11];
	
	private void initArguments() {
		arguments[0] = "installframework";
		arguments[1] = "-dbconnpropertyfile";
		arguments[3] = "-dbtype";
		arguments[5] = "-module";
		arguments[7] = "-parameternames";
		arguments[8] = "p#frm#";
		arguments[9] = "-parametervalues";
		arguments[10] = "FRM_";
	}

	@Test
	public void testInstallFramework() {
		
		initArguments();
		
		arguments[2] = "localhost_oracle_dwhdev_dwhadmin_test";
		arguments[4] = "oracle/adminusr";
		// Perform test
		try {
			arguments[6] = "tool";
			Main.main(arguments);
			arguments[6] = "trac";
			Main.main(arguments);
			arguments[6] = "taxn";
			Main.main(arguments);
			arguments[6] = "mesr";
			Main.main(arguments);
			arguments[6] = "stag";
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
