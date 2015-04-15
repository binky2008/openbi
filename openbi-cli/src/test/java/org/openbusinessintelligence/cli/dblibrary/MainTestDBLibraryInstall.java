package org.openbusinessintelligence.cli.dblibrary;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;
import org.openbusinessintelligence.cli.prepare.PrepareSchemaHelper;

public class MainTestDBLibraryInstall {
	
	private String[] arguments = new String[15];
	
	private void prepareArguments() {
		arguments[0]  = "installdblibrary";
		arguments[1]  = "-dbconnpropertyfile";
		arguments[3]  = "-dbproduct";
		arguments[5]  = "-dbcatalog";
		arguments[7]  = "-dbschema";
		arguments[9]  = "-module";
		arguments[11] = "-parameternames";
		arguments[13] = "-parametervalues";
	}

	@Test
	public void testOracle() {
		// Perform test
		prepareArguments();
		arguments[2]  = "localhost_oracle_dwhdev_dwhadmin_test";
		arguments[4]  = "oracle";
		arguments[6]  = "";
		arguments[8]  = "adminusr";
		arguments[10] = "all";
		arguments[12] = "p#frm#";
		arguments[14] = "dwhutl_";
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
