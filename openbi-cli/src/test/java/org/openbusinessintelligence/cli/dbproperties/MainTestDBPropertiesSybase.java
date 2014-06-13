package org.openbusinessintelligence.cli.dbproperties;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestDBPropertiesSybase {
	
	private String[] arguments = new String[5];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "dbproperties";
		// Mandatory arguments
		arguments[1] = "-dbconnpropertyfile";
		// Optional arguments
		arguments[3] = "-dbconnkeywordfile";
		
	}

	@Test
	public void testSybase() {
		
		initArguments();
		//
		arguments[2] = "localhost_sybase_test";
		arguments[4] = "";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
