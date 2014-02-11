package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBPropertiesNoPropertyFile {
	
	private String[] arguments = new String[10];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "dbproperties";
		// Mandatory arguments
		arguments[2] = "-dbdriverclass";
		arguments[4] = "-dbconnectionurl";
		arguments[6] = "-dbusername";
		arguments[8] = "-dbpassword";
		
	}

	@Test
	public void testHANA () {
		
		initArguments();
		//
		arguments[3] = "com.sap.db.jdbc.Driver";
		arguments[5] = "jdbc:sap://msas120i:30115/HID";
		arguments[7] = "sugarcrm";
		arguments[9] = "Sommer2013!";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	/*@Test
	public void testTeradata () {
		
		initArguments();
		//
		arguments[3] = "com.ncr.teradata.TeraDriver";
		arguments[5] = "jdbc:teradata://tdexpress1410_sles11/dbc";
		arguments[7] = "dbc";
		arguments[9] = "dbc";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}*/
}
