package org.openbusinessintelligence.cli;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.openbusinessintelligence.cli.helper.MainTestCopySchemaHelper;

public class MainTestInstallFrameworkOracle {
	
	private String[] arguments = new String[7];
	
	private void initArguments() {
		arguments[0] = "installframework";
		arguments[1] = "-dbconnpropertyfile";
		arguments[3] = "-dbtype";
		arguments[5] = "-module";
	}

	@Test
	public void testInstallFramework() {
		
		initArguments();
		
		arguments[2] = "localhost_oracle_dwhadmin";
		arguments[4] = "oracle";
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