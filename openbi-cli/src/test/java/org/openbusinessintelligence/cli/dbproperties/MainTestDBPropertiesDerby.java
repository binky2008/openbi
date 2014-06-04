package org.openbusinessintelligence.cli.dbproperties;

import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.cli.Main;

public class MainTestDBPropertiesDerby {
	
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
	public void testDerby() {
		
		initArguments();
		//
		arguments[2] = "localhost_derby_sample";
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
