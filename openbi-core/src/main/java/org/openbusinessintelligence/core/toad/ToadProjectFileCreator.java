package org.openbusinessintelligence.core.toad;

import java.io.*;

import org.openbusinessintelligence.core.file.*;
import org.slf4j.LoggerFactory;


/**
 * Class for creation of TOAD project files
 * @author Nicola Marangoni
 */
public class ToadProjectFileCreator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ToadProjectFileCreator.class);

	private static String projectTree;
	private static String startTabs = "";
	private static FileExporter projectFile = new FileExporter();

	// Create a project file
	public void createProject(String projectName, String projectDirectory, String rootDirectory) throws Exception {
		projectTree = "2," + projectName;
		projectFile.setDirectoryName(projectDirectory);
		projectFile.setFileName(projectName + ".tpr");
		logger.debug("Project name: " + projectName);
		logger.debug("Project folder: " + projectDirectory);
		logger.debug("Project folder: " + rootDirectory);
		Writer writer = projectFile.getWriter();
		try {
			writer.write(projectTree + "\n");
			logger.debug(projectTree);
			createTree(writer,rootDirectory,startTabs);
			writer.close();
		}
		catch (Exception e) {
			System.out.println(e);
			throw e;
		}
	}

	// Loop recursively in the directory tree in order to add al files to the project
	public static void createTree(Writer writer, String subDir, String tabs) throws IOException {
		tabs += "\t";
		File file = new File(subDir);
		File[] strFilesDirs = file.listFiles();
		for ( int i = 0 ; i < strFilesDirs.length ; i ++ ) {
			String path = strFilesDirs[i].toString();
			String[] arrPath = path.split("\\\\");
			String name = arrPath[arrPath.length - 1];
			if (strFilesDirs[i].isDirectory()) {
				logger.debug("Path: " + path);
				if (!(name.equals(".svn"))) {
					writer.write(tabs + "3," + name + "," + path + ",FALSE,*.*\n");
					logger.debug(tabs + "3," + name + "," + path + ",FALSE,*.*");
					projectTree += tabs + "3," + name + "," + path + ",FALSE,*.*";
					createTree(writer,path,tabs);
				}
			}
		}
		for ( int i = 0 ; i < strFilesDirs.length ; i ++ ) {
			String path = strFilesDirs[i].toString();
			String[] arrPath = path.split("\\\\");
			String name = arrPath[arrPath.length - 1];
			if (strFilesDirs[i].isFile()) {
				writer.write(tabs + "6," + name + "," + path + "\n");
				logger.debug(tabs + "6," + name + "," + path);
				projectTree += tabs + "6," + name + "," + path;
			}
		}
	}
}
