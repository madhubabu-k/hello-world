package com.pearson.ps.ingest.utils;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author HCLT
 * 
 */
public class ActiveMQUtils {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ActiveMQUtils.class);

	/**
	 * Method to post new items to the queue.
	 * 
	 * @param queueName
	 *            Name of the Queue
	 * @param myMsg
	 *            message to be posted
	 * @param URL
	 *            URL of the ActiveMQ server Sample tcp://activemqsvr:61616
	 * @throws JMSException
	 */
	public static void publishMessageToQueue(String queueName, String myMsg,
			String URL) throws JMSException {

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// Create new Session
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);

		Destination destination = session.createQueue(queueName);

		MessageProducer producer = session.createProducer(destination);

		TextMessage message = session.createTextMessage(myMsg);

		producer.send(message);
		LOGGER.info("Message Posted :: '{}'", message.getText());

		connection.close();

	}

	/**
	 * Method to retrive the latest message from the queue
	 * 
	 * @param queueName
	 *            Queue Name
	 * @param URL
	 *            URL of the ActiveMQ server Sample tcp://activemqsvr:61616
	 * @return Message as String
	 * @throws JMSException
	 */
	@SuppressWarnings("unchecked")
	public static String browseMessageFromQueue(String queueName, String URL)
			throws JMSException {
		TextMessage textMessage = null;
		String messageString = "";

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);

		Destination destination = session.createQueue(queueName);

		QueueBrowser browser = session.createBrowser((Queue) destination);
		Enumeration enumMessages = browser.getEnumeration();
		if (enumMessages.hasMoreElements()) {
			textMessage = (TextMessage) enumMessages.nextElement();
			messageString = textMessage.getText();
			LOGGER.debug("Message browsed :: '{}'", messageString);
		}
		connection.close();
		return messageString;
	}

	/**
	 * Method to retrive the latest message from the queue
	 * 
	 * @param queueName
	 *            Queue Name
	 * @param URL
	 *            URL of the ActiveMQ server Sample tcp://activemqsvr:61616
	 * @return Message as String
	 * @throws JMSException
	 */
	public static String retrieveMessageFromQueue(String queueName, String URL)
			throws JMSException {

		String messageString = "";

		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);

		Destination destination = session.createQueue(queueName);

		MessageConsumer consumer = session.createConsumer(destination);
		// Setting timeout to 2 secs
		Message message = consumer.receive(2000);

		if (message instanceof TextMessage) {
			TextMessage textMessage = (TextMessage) message;
			messageString = textMessage.getText();
			LOGGER.debug("Message Received :: '{}'", textMessage.getText());
		}
		connection.close();
		return messageString;
	}

}