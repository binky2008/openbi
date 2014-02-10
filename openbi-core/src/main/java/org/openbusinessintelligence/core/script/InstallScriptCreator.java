package org.openbusinessintelligence.core.script;

import java.io.*;

import org.openbusinessintelligence.core.file.*;
import org.slf4j.LoggerFactory;

public class InstallScriptCreator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(InstallScriptCreator.class);

	private static FileInputBean installFile = new FileInputBean();
	private static FileOutputBean scriptFile = new FileOutputBean();

	public void createScript(String scriptName, String scriptDirectory, String rootDirectory, String installFileName) throws Exception {
		
		scriptFile.setDirectoryName(scriptDirectory);
		scriptFile.setFileName(scriptName + ".sql");		

		System.out.println("Install file = " + rootDirectory + File.separatorChar + installFileName + ".sql");
		installFile.setDirectoryName(rootDirectory);
		installFile.setFileName(installFileName + ".sql");
		BufferedReader reader = installFile.getReader();
		String installLine;
		
		Writer writer = scriptFile.getWriter();
		try {
			writer.write("-- Deployment script " + scriptName + "\n");
			while ((installLine=reader.readLine()) != null) {
				if (installLine.length()>0) {
					if (installLine.trim().substring(0,1).equals("@")) {
						installLine = installLine.trim().substring(1);
						if (installLine.substring(installLine.length()-1).equals(";")) {
							installLine = installLine.substring(0,installLine.length()-1);
						}
						System.out.println("Script " + installLine + " added");
						FileInputBean objectFile = new FileInputBean();
						objectFile.setFilePath(rootDirectory + File.separatorChar + installLine);
						writer.write("\n--Next object\n");
						writer.write(objectFile.getString());
					}
				}
			}
			writer.close();
		}
		catch (Exception e) {
			System.out.println(e);
			System.out.println("File " + rootDirectory + File.separatorChar + "install.sql has problems!");
		}
		System.out.println("Finished");
	}
}
