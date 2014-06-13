package org.openbusinessintelligence.cli.generatedata;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestGenerateDataDB2 {
	
	private String[] arguments = new String[11];
	
	public void initArguments() {
		
		// Function to test
		arguments[0]  = "generaterandomdata";
		// Mandatory arguments
		arguments[1]  = "-trgdbconnpropertyfile";
		arguments[3] = "-trgdbconnkeywordfile";
		arguments[5] = "-targetschema";
		arguments[7] = "-targettable";
		//
		arguments[9] = "-numberofrows";
	}
	
	public void initSource() {
		// Target properties
		arguments[2] = "localhost_db2_dwhdev_test";
		arguments[4] = "";
		arguments[6] = "test";
		arguments[8] = "tab_test";
		arguments[10] = "1000";
	}

	@Test
	public void testDB2() {
		
		initArguments();
		initSource();
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}