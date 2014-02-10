package org.openbusinessintelligence.core.xml;

import java.io.*;

import javax.xml.transform.*;
import javax.xml.transform.stream.*;

import org.slf4j.LoggerFactory;

/**
 * Transform an XML input into an XML output using a XSL stylesheet
 * @author marangon
 */
public class TransformerBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TransformerBean.class);

	private Reader styleSheet;
	private Writer xmlOutput;

	private StreamSource streamSource;
	private StreamResult streamResult;

	public TransformerBean() {
		super();
	}

	public void setStyleSheet (Reader ss) {
		styleSheet = ss;
	}

	public void setXmlInput (Reader xi) {
		streamSource = new StreamSource(xi);
	}

	public void setXmlOutput (Writer wr) {
		streamResult = new StreamResult(wr);
	}

	public void setStreamInput (ByteArrayInputStream in) {
		streamSource = new StreamSource(in);
	}

	public void setStreamOutput (ByteArrayOutputStream rs) {
		streamResult = new StreamResult(rs);
	}

	public Writer getXmlOutput () {
		return xmlOutput;
	}

	// Transform XML into another XML using a stylesheet
	public void transform() {
		try {
			TransformerFactory transFact = TransformerFactory.newInstance();
			Transformer trans = transFact.newTransformer(new StreamSource(styleSheet));
			trans.setOutputProperty(OutputKeys.METHOD, "xml");
			trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"no");
			trans.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			trans.setOutputProperty(OutputKeys.INDENT, "yes");
			trans.transform(streamSource, streamResult);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
