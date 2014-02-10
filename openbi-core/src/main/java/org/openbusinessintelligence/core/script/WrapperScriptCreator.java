package org.openbusinessintelligence.core.script;

import java.io.*;

import org.openbusinessintelligence.core.file.*;
import org.slf4j.LoggerFactory;


/**
 * Class for creation of TOAD project files
 * @author Nicola Marangoni
 */
public class WrapperScriptCreator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(WrapperScriptCreator.class);

	private static String rootDir = "";
	private static FileOutputBean wrapperFile = new FileOutputBean();

	// Create a project file
	public void createWrapper(String scriptName, String rootDirectory, String defSubDirs) throws Exception {
		
		rootDir = rootDirectory;
		wrapperFile.setDirectoryName(rootDirectory);
		wrapperFile.setFileName(scriptName + ".sql");
		Writer writer = wrapperFile.getWriter();
		try {
			if ((defSubDirs == null) || (defSubDirs.equals(""))) {
				createWrapper(writer,rootDirectory);
			}
			else {
				String[] arrSubDirs = defSubDirs.split(",");
				for (int i=0; i < arrSubDirs.length; i++) {
					writer.write("-- " + arrSubDirs[i] + "\n");
					System.out.println("-- " + arrSubDirs[i] + "\n");
					createWrapper(writer,rootDirectory + "\\" + arrSubDirs[i]);
				}
			}
			writer.close();
		}
		catch (Exception e) {
			System.out.println(e);
			throw e;
		}
		System.out.println("Finished");
	}

	// Loop recursively in the directory tree in order to add al files to the project
	public static void createWrapper(Writer writer, String subDir) throws IOException {
		File file = new File(subDir);
		File[] strFilesDirs = file.listFiles();
		if (!(strFilesDirs==null)) {
		for ( int i = 0 ; i < strFilesDirs.length ; i ++ ) {
			String path = strFilesDirs[i].toString();
			String[] arrPath = path.split("\\\\");
			String name = arrPath[arrPath.length - 1];
			if (strFilesDirs[i].isDirectory()) {
				if (!(name.equals(".svn"))) {
					writer.write("-- " + name + "\n");
					System.out.println("-- " + name + "\n");
					createWrapper(writer,path);
				}
			}
		}
		for ( int i = 0 ; i < strFilesDirs.length ; i ++ ) {
			String path = strFilesDirs[i].toString();
			if (strFilesDirs[i].isFile()) {
				writer.write("@" + path.replace(rootDir + "\\", "") + ";\n");
				System.out.println("@" + path.replace(rootDir + "\\", "") + ";\n");
			}
		}
		}
	}
}
