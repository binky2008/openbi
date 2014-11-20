package org.openbusinessintelligence.install;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import org.slf4j.LoggerFactory;

public class ParameterReplacer {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ParameterReplacer.class);

	private String[] stringsToReplace;
	private String[] stringsReplacement;
	private Reader sourceReader;
	
    // Constructor
    public ParameterReplacer() {
        super();
    }
    
    public void setStringToReplace(String[] property) {
    	stringsToReplace = property;
    }
    
    public void setStringReplacement(String[] property) {
    	stringsReplacement = property;
    }
    
    public void setSourceReader(Reader property) {
    	sourceReader = property;
    }
	
    public Reader getTargetReader() throws IOException {
    	
    	char[] buff = new char[1024];
    	Writer stringWriter = new StringWriter();
    	int n;
    	try {
	    	while ((n = sourceReader.read(buff)) != -1) {
	    		stringWriter.write(buff, 0, n);
	    	}
    	}
    	finally {
    		stringWriter.close();
    	}
    	String targetString = stringWriter.toString();
    	logger.debug("Before replacement:\n" + targetString);

    	if (
    		!(stringsToReplace==null) &&
    		!(stringsReplacement==null)
    	)
 		for (int i = 0; i < stringsToReplace.length; i++) {
 	    	targetString = targetString.replaceAll(stringsToReplace[i], stringsReplacement[i]);
 		}

    	logger.debug("After replacement:\n" + targetString);
    	
    	StringReader targetReader = new StringReader(targetString);
    	
    	return targetReader;
    }
}
