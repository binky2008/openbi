package org.openbusinessintelligence.cli.copy.table;

import org.openbusinessintelligence.cli.Main;

public class MainTestCopyTableHelper {

	private static String[] arguments = new String[21];
	
	public static void initArguments() {
		
		// Function to test
		arguments[0]  = "tablecopy";
		// Source and target properties
		arguments[1]  = "-srcdbconnpropertyfile";
		arguments[3]  = "-srcdbconnkeywordfile";
		arguments[5]  = "-sourceschema";
		arguments[7]  = "-sourcetable";
		arguments[9]  = "-trgdbconnpropertyfile";
		arguments[11]  = "-trgdbconnkeywordfile";
		arguments[13] = "-targetschema";
		arguments[15] = "-targettable";
		// Common options
		arguments[17] = "-trgcreate";
		arguments[18] = "true";
		arguments[19] = "-dropifexists";
		arguments[20] = "true";
		
	}
	
	public static void initSource(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[2 + i * 2] = values[i];
		}
		
	}
	
	public static void initTarget(String[] values) {
		
		for (int i = 0; i < values.length; i++) {
			arguments[10 + i * 2] = values[i];
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
