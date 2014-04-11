package org.openbusinessintelligence.cli;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBPropertiesNoPropertyFile {
	
	private String[] arguments = new String[9];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "dbproperties";
		// Mandatory arguments
		arguments[1] = "-dbdriverclass";
		arguments[3] = "-dbconnectionurl";
		arguments[5] = "-dbusername";
		arguments[7] = "-dbpassword";
		
	}

	@Test
	public void testHANA () {
		
		initArguments();
		//
		arguments[2] = "com.sap.db.jdbc.Driver";
		arguments[4] = "jdbc:sap://msas120i:30115/HID";
		arguments[6] = "sugarcrm";
		arguments[8] = "Sommer2013!";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testTeradata () {
		
		initArguments();
		//
		arguments[2] = "com.ncr.teradata.TeraDriver";
		arguments[4] = "jdbc:teradata://TDExpress1410_Sles11.i.msg.de/dbc";
		arguments[6] = "dbc";
		arguments[8] = "dbc";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
