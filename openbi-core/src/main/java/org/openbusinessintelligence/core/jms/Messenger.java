package org.openbusinessintelligence.core.jms;

import java.util.*;
import javax.jms.*;
import javax.naming.*;

import org.slf4j.LoggerFactory;

/**
 * Utility class to facilitate the interaction with a jms server
 * @author marangon
 */
public class Messenger {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(Messenger.class);
	
    // Declarations of bean properties
    private Properties serverEnvironment;
    private String connectionFactoryName = null;
    private String queueName = null;
    private String messageText = null;
    private long timeout;

    // Declarations of internally used variables
    private Context jndiContext;
    private QueueConnectionFactory connectionFactory = null;
    private String userName = "";
    private String passWord = "";
    private QueueConnection connection = null;
    private QueueSession session = null;
    private javax.jms.Queue queue = null;
    private MessageProducer producer = null;
    private MessageConsumer consumer = null;

    // Constructor
    public Messenger() {
        super();
    }

    // Set methods
    public void setServerEnvironment(Properties se) {
        serverEnvironment = se;
    }

    public void setConnectionFactoryName (String cfn) {
        connectionFactoryName = cfn;
    }

    public void setUserName(String un) {
        userName = un;
    }

    public void setPassWord(String pw) {
        passWord = pw;
    }

    public void setQueueName (String qn) {
        queueName = qn;
    }

    public void setMessageText (String mt) {
        messageText = mt;
    }

    public void setTimeout(long to) {
        timeout = to;
    }

    public String getMessageText () {
        return messageText;
    }

    // Context methods
    public void initialize() throws NamingException  {
        jndiContext = new InitialContext(serverEnvironment);
        System.out.println("Context created.");
        connectionFactory = (QueueConnectionFactory)jndiContext.lookup(connectionFactoryName);
        System.out.println("Connection factory found.");

        try {
        	queue = (javax.jms.Queue)jndiContext.lookup(queueName);
        }
        catch (Exception e) {
            System.out.println("Queue not found.");
        }
        System.out.println("Queue created.");
        System.out.println("");
    }

    public void finalize() throws NamingException {
        jndiContext.close();
        System.out.println("Context closed.");
        System.out.println("");
    }

    // Messaging methods
    public void produce() throws JMSException {
        System.out.println("PRODUCING MESSAGE...");

        // Prepare connection and session
        connection = connectionFactory.createQueueConnection(userName, passWord);
        connection.start();
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        System.out.println("Session created.");
        if (queue == null) {
        	queue = session.createQueue(queueName);
        }

        // Prepare message
        producer = session.createProducer(queue);
        System.out.println("Producer created.");
        TextMessage message = session.createTextMessage();
        message.setText(messageText);

        // Send message
        producer.send(message);
        System.out.println("Message sent.");

        // Close everything
        producer.close();
        session.close();
        connection.close();
        System.out.println("Connection closed.");
        System.out.println("MESSAGE PRODUCED");
        System.out.println("");
    }

    public void consume() throws JMSException {
        System.out.println("CONSUMING MESSAGE...");
        connection = connectionFactory.createQueueConnection();
        connection.start();
        System.out.println("Connection created.");
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        if (queue == null) {
        	queue = session.createQueue(queueName);
        }
        System.out.println("Session created.");
        consumer = session.createConsumer(queue);
        System.out.println("Consumer created.");
        TextMessage message = (TextMessage)consumer.receive(timeout);
        if (message != null) {
            System.out.println("Message received.");
        	messageText = message.getText();
        }
        else {
            System.out.println("Message NOT received.");
        }
        consumer.close();
        session.close();
        connection.close();
        System.out.println("Connection closed.");
        System.out.println("MESSAGE CONSUMED");
        System.out.println("");
    }

    public void testConnection()  {
        try {
            jndiContext = new InitialContext(serverEnvironment);
            System.out.println("Context created.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Context creation failed.");
        }
        try {
            connectionFactory = (QueueConnectionFactory)jndiContext.lookup(connectionFactoryName);
            System.out.println("Connection factory created.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Factory lookup failed.");
        }
        try {
            queue = (javax.jms.Queue)jndiContext.lookup(queueName);
            System.out.println("Queue created.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Queue lookup failed.");
        }
        try {
            // Prepare connection and session
            connection = connectionFactory.createQueueConnection(userName, passWord);
            connection.start();
            System.out.println("Connection created.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Connection failed.");
        }
        try {
            session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            System.out.println("Session created.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Session failed.");
        }
        try {
            session.close();
            System.out.println("Session closed.");
        }
        catch  (Exception e) {
            System.out.println(e);
            System.out.println("Close session failed.");
        }
        try {
            connection.close();
            System.out.println("Connection closed.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Close connection failed.");
        }
        try {
            jndiContext.close();
            System.out.println("Context closed.");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Close context failed.");
        }
        System.out.println("");
    }
}