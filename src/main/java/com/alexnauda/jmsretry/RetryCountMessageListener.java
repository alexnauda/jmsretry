package com.alexnauda.jmsretry;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.apache.log4j.Logger;
import org.springframework.jms.core.JmsTemplate;

/**
 * This is a message listener proxy that implements something similar to the backout/retry queue functionality offered by some JMS servers
 * to work around the "poison" message problem, in which a single message can cause message consumer processing to fail in an infinite
 * loop.
 *  
 * OpenMQ is slated to offer this support in version 4.6. I mimic this functionality by requiring objects passed via ObjectMessage to
 * implement the RetryCount interface to basically embed the retry count into the payload itself.
 * 
 * Aim the retryJmsTemplate at this listener's own input queue. Messages that have been retried less than maxRetries will be requeued there.
 * 
 * Create an error queue, and aim errorJmsTemplate at it. Messages that have exceeded maxRetries will be queued there for exception
 * processing and/or later manual review.
 * 
 * Example spring context:
 * 
	<bean id="requestQueue" class="com.sun.messaging.Queue" lazy-init="true">
		<constructor-arg type="java.lang.String" value="request" />
	</bean>

	<bean id="requestErrorQueue" class="com.sun.messaging.Queue" lazy-init="true">
		<constructor-arg type="java.lang.String" value="request_error" />
	</bean>

 	<bean id="connectionFactoryFactory" class="com.alexnauda.jmsretry.MQConnectionFactoryFactory" lazy-init="true">
		<property name="properties">
			<props>
				<prop key="imqAddressList">localhost:7676</prop>
			</props>
		</property>
	</bean>

	<bean id="connectionFactory" factory-bean="connectionFactoryFactory" factory-method="createConnectionFactory" lazy-init="true"/>

	<bean id="springConnectionFactory" class="org.springframework.jms.connection.SingleConnectionFactory" lazy-init="true">
		<property name="targetConnectionFactory" ref="connectionFactory" />
	</bean>
	
	<bean id="responseContainer" class="org.springframework.jms.listener.DefaultMessageListenerContainer" lazy-init="true">
		<property name="connectionFactory" ref="springConnectionFactory" />
		<property name="destination" ref="responseQueue" />
		<property name="messageListener" ref="responseListener" />
		<property name="autoStartup" value="false" />
		<property name="sessionTransacted" value="true" />
	</bean>
	
	<bean id="responseErrorJmsTemplate" class="org.springframework.jms.core.JmsTemplate" lazy-init="true">
		<property name="connectionFactory" ref="springConnectionFactory" />
		<property name="defaultDestination" ref="responseErrorQueue" />
		<property name="sessionTransacted" value="true" />
	</bean>
	
	<bean id="responseListener" class="com.alexnauda.jmsretry.RetryCountMessageListener">
		<constructor-arg>
			<bean class="your.package.ResponseListener">
				<property name="jdbcTemplate" ref="jdbcTemplate" />
			</bean>
		</constructor-arg>
		<property name="errorJmsTemplate" ref="responseErrorJmsTemplate" />
		<property name="retryJmsTemplate" ref="responseJmsTemplate" />
		<property name="maxRetries" value="5" />
	</bean>
	
	<bean id="responseProcessor" class="your.package.ResponseProcessor" lazy-init="true"/>
	
 * @author alex
 *
 */
public class RetryCountMessageListener implements MessageListener {

	private static final Logger log = Logger.getLogger(RetryCountMessageListener.class);

	protected MessageListener messageListener;

	public RetryCountMessageListener(final MessageListener _messageListener) {
		messageListener = _messageListener;
	}

	public static String RETRIES_PROPERTY_NAME = "X-Retries";

	public static int DEFAULT_MAX_RETRIES = 5;

	protected JmsTemplate errorJmsTemplate;

	protected JmsTemplate retryJmsTemplate;

	protected int maxRetries = DEFAULT_MAX_RETRIES;

	public JmsTemplate getErrorJmsTemplate() {
		return errorJmsTemplate;
	}

	public void setErrorJmsTemplate(final JmsTemplate errorJmsTemplate) {
		this.errorJmsTemplate = errorJmsTemplate;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(final int maxRetries) {
		this.maxRetries = maxRetries;
	}

	public void onMessage(final Message message) {
		try {
			messageListener.onMessage(message);
		} catch (final RuntimeException e) {
			log.error("Error in message listener", e);
			int retries = 0;
			retries = getRetries(message);
			log.info("Number of retries for this message is " + retries + ". Max retries is configured to " + maxRetries + ".");
			if (retries + 1 > maxRetries) {
				log.error("Number of retries (" + retries + ") reached max retries (" + maxRetries + "). Sending to error queue.");
				final ObjectMessage om = (ObjectMessage) message;
				try {
					final Object o = om.getObject();
					errorJmsTemplate.convertAndSend(o);
					// TODO consider pausing for several minutes if there are too many failed messages
					return; // return cleanly so that this message is removed from the request queue
				} catch (final JMSException f) {
					throw new RuntimeException("Number of retries (" + retries + ") reached max retries (" + maxRetries
							+ "), but failed to send message to error queue", f);
				}
			} else {
				// increment retries and send to retry queue
				final Object o = setRetries(message, retries + 1);
				retryJmsTemplate.convertAndSend(o);
			}
			/* if anything in this catch block throws an exception, the JMS server should requeue the message automatically, but without incrementing
			 * the retry count
			 */
			// TODO replace with server-enabled retry counting after it's available in OpenMQ (planned for version 4.6)
		}
	}

	private Object setRetries(final Message message, final int retries) {
		try {
			if (message instanceof ObjectMessage) {
				final ObjectMessage om = (ObjectMessage) message;
				final Object o = om.getObject();
				if (o instanceof RetryCount) {
					((RetryCount) o).setRetryCount(retries);
					return o;
				}
			}
		} catch (final JMSException e) {
			throw new RuntimeException("Could not increment retry count property for name " + RETRIES_PROPERTY_NAME, e);
		}
		throw new RuntimeException("Could not set retry count of message " + message);
	}

	private int getRetries(final Message message) {
		try {
			if (message instanceof ObjectMessage) {
				final ObjectMessage om = (ObjectMessage) message;
				final Object o = om.getObject();
				if (o instanceof RetryCount) {
					return ((RetryCount) o).getRetryCount();
				} else {
					throw new RuntimeException("Object must be an instance of RetryCount. Could not get retry count of object: " + o);
				}
			} else {
				throw new RuntimeException("Message must be an instance of ObjectMessage. Could not get retry count of message: " + message);
			}
		} catch (final JMSException e) {
			throw new RuntimeException("Could not get object from ObjectMessage", e);
		}
	}

	public JmsTemplate getRetryJmsTemplate() {
		return retryJmsTemplate;
	}

	public void setRetryJmsTemplate(final JmsTemplate retryJmsTemplate) {
		this.retryJmsTemplate = retryJmsTemplate;
	}

}
