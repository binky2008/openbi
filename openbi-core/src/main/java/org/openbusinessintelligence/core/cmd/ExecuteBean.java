package org.openbusinessintelligence.core.cmd;

import java.io.*;

import org.slf4j.*;

/**
 * This class contains methods to execute os/command line instructions.
 * It allows also to build the complete instruction passing flags and/or arguments
 * @author marangon
 */
public class ExecuteBean {
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ExecuteBean.class);

	private String commandName = "";
	private String[] commandFlags = null;
	private String[] commandArgumentNames = null;
	private String[] commandArgumentValues = null;
	private String output = "Failure";
	
	/**
	 * Contructor class
	 */
	public ExecuteBean() {
		super();
	}
	
	/**
	 * Set the command name (instruction to execute)
	 */
	public void setCommandName(String cn) {
		commandName = cn;
	}
	
	/**
	 * Set the list of flags in form of an array
	 */
	public void setCommandFlags(String[] cfl) {
		commandFlags = cfl;
	}
	
	/**
	 * Set the list of argument names in form of an array
	 */
	public void setArgumentNames(String[] can) {
		commandArgumentNames = can;
	}
	
	/**
	 * Set the list of argument values in form of an array
	 */
	public void setArgumentValues(String[] cav) {
		commandArgumentValues = cav;
	}
	
	/**
	 * Excecute the os command and return the eventual output
	 */
	public String getOutput() {
		
		String commandLine = commandName;
		
		// Initialize runtime and process objects
		Runtime rt;
		Process ps = null;
		
		// Contruct the complete command line instruction (combining command, flags and argument-name pairs)
		try {
			for (int i = 0; i < commandFlags.length; i++) {
				commandLine += " -" + commandFlags[i];
			}
		}
		catch(NullPointerException e) {
			System.out.println("No flags dedebugd");
		}
		try {
			for (int i = 0; i < commandArgumentValues.length; i++) {
				try {
					commandLine += " -" + commandArgumentNames[i] + " " + commandArgumentValues[i];
				}
				catch(Exception e) {
					commandLine += " " + commandArgumentValues[i];
				}
			}
		}
		catch(NullPointerException e) {
			System.out.println("No arguments dedebugd");
		}

		System.out.println(commandLine);
		
		// Launch the instruction
		try {
			rt = Runtime.getRuntime();
			ps = rt.exec(commandLine);
		}
		catch(IOException e) {
			System.out.println("Error launching process");
		}
		

		
		// Wait for finish
		try {
	        ps.waitFor();
			output = "Success";
		}
		catch(InterruptedException e) {
			System.out.println("Error executing process");
		}
		
		return output;
	}
	
}