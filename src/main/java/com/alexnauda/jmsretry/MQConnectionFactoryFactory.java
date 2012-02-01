package com.alexnauda.jmsretry;

import java.util.Enumeration;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.log4j.Logger;

public class MQConnectionFactoryFactory {

	private static final Logger log = Logger.getLogger(MQConnectionFactoryFactory.class);

	private Properties props;

	public void setProperties(final Properties props) {
		this.props = props;
	}

	public ConnectionFactory createConnectionFactory() throws JMSException {
		final com.sun.messaging.ConnectionFactory cf = new com.sun.messaging.ConnectionFactory();
		final Enumeration<?> keys = props.propertyNames();
		while (keys.hasMoreElements()) {
			final String name = (String) keys.nextElement();
			final String value = props.getProperty(name);
			log.info("Setting up connection factory for " + name + "=" + value);
			cf.setProperty(name, value);
		}
		return cf;
	}
}