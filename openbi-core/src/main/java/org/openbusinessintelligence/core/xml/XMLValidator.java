package org.openbusinessintelligence.core.xml;
/**
 * Validate an XML file or a given XML string
 * @author marangon
 */

import java.io.*;
import javax.xml.validation.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.*;

import org.slf4j.LoggerFactory;
import org.xml.sax.*;

/**
 * Class for validation of XML files
 * @author Nicola Marangoni
 */
public class XMLValidator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(XMLValidator.class);

	private String schemaFileName = null;
	private String sourceFileName = null;
	private String sourceString = null;

	public void setSchemaFileName (String sfn) {
		schemaFileName = sfn;
	}

	public void setSourceFileName (String sfn) {
		sourceFileName = sfn;
	}

	public void setSourceString (String ss) {
		sourceString = ss;
	}

	// Validate an XML file
	public void validate() throws SAXException, IOException {
		Source source = null;
		SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
		File schemaLocation = new File(schemaFileName);
		Schema schema = factory.newSchema(schemaLocation);
		Validator validator = schema.newValidator();
		if (sourceString != null) {
			StringReader sReader = new StringReader(sourceString);
			source = new StreamSource(sReader);
		}
		else if (sourceFileName != null) {
			source = new StreamSource(sourceFileName);
		}
		validator.validate(source);
	}

}
