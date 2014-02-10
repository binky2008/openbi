package org.openbusinessintelligence.core.mail;

import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

import org.slf4j.LoggerFactory;


/**
 * Class for sending of emails
 * @author Nicola Marangoni
 */
public class MailBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(MailBean.class);

	private String smtpServer;
	private String senderAddress;
	private String receiverList;
	private String mailSubject;
	private String mailContent;

	/**
	 * Constructor
	 */
	public MailBean() {
		super();
	}

	/**
	 * Set the smtp server
	 */
	public void setSmtpServer(String sv) {
		smtpServer = sv;
	}

	/**
	 * Set the sender address
	 */
	public void setSenderAddress(String sa) {
		senderAddress = sa;
	}

	/**
	 * Set the receiver list
	 */
	public void setReceiverList(String rl) {
		receiverList = rl;
	}

	/**
	 * Set the mail subject
	 */
	public void setMailSubject(String ms) {
		mailSubject = ms;
	}

	/**
	 * Set the mail content
	 */
	public void setMailContent(String mc) {
		mailContent = mc;
	}
	
	// Send an email
	public void sendMail() {
		logger.info("Sending email");
		Properties properties = System.getProperties();
		properties.setProperty("mail.smtp.host", smtpServer);
		Session session = Session.getDefaultInstance(properties);
		MimeMessage message = new MimeMessage(session);
		try {
			message.setFrom(new InternetAddress(senderAddress));
			message.addRecipient(Message.RecipientType.TO,  new InternetAddress(receiverList));
			message.setSubject(mailSubject);
			message.setContent(mailContent,"text/html");
			
			Transport.send(message);
			logger.info("Email sent");
		}
		catch (Exception e) {
			logger.error("Cannot send email:\n" + e.getMessage());
		}
	}
	
}
