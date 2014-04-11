package org.openbusinessintelligence.cli;

import static org.junit.Assert.fail;

public class MainTestCopyTableHelper {

	private static String[] arguments = new String[19];
	
	public static void initArguments() {
		
		// Function to test
		arguments[0]  = "tablecopy";
		// Source and target properties
		arguments[1]  = "-srcdbconnpropertyfile";
		arguments[3]  = "-srcdbconnkeywordfile";
		arguments[5]  = "-sourcetable";
		arguments[7]  = "-trgdbconnpropertyfile";
		arguments[9]  = "-trgdbconnkeywordfile";
		arguments[11] = "-targetschema";
		arguments[13] = "-targettable";
		// Common options
		arguments[15] = "-trgcreate";
		arguments[16] = "true";
		arguments[17] = "-dropifexists";
		arguments[18] = "true";
		
	}
	
	public static void initSource(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[2 + i * 2] = values[i];
		}
		
	}
	
	public static void initTarget(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[8 + i * 2] = values[i];
		}
		
	}
	
	public static void execute() throws Exception {
		// Perform test
		initArguments();
		for (int i = 0; i < arguments.length; i++) {
			System.out.println(arguments[i]);
		}
		Main.main(arguments);
		
	}

}
