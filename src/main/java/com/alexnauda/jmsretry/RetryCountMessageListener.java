package com.alexnauda.jmsretry;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jms.core.JmsOperations;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.util.Assert;

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
public class RetryCountMessageListener implements MessageListener, InitializingBean {

	private static final Logger log = Logger.getLogger(RetryCountMessageListener.class);

	protected MessageListener messageListener;

	public RetryCountMessageListener(final MessageListener _messageListener) {
		this.messageListener = _messageListener;
	}

	private static final String RETRIES_PROPERTY_NAME = "RetryCount";

	private static final int DEFAULT_MAX_RETRIES = 5;

	private JmsOperations errorJmsOperations;

	private Destination errorDestination;

	private JmsOperations retryJmsOperations;

	private Destination retryDestination;

	protected int maxRetries = DEFAULT_MAX_RETRIES;

	public void onMessage(final Message message) {
		try {
			this.messageListener.onMessage(message);
		} catch (final RuntimeException e) {
			log.error("Error in message listener", e);
			int retries = 0;
			retries = getRetries(message);
			log.info("Number of retries for this message is " + retries + ". Max retries is configured to " + this.maxRetries + ".");
			final MessageCreator messageCreator = new MessageCreator() {
				public Message createMessage(final Session arg0) throws JMSException {
					return message;
				}
			};
			if (retries + 1 > this.maxRetries) {
				log.error("Number of retries (" + retries + ") reached max retries (" + this.maxRetries + "). Sending to error queue.");
				if (this.errorDestination == null) {
					this.errorJmsOperations.send(messageCreator);
				} else {
					this.errorJmsOperations.send(this.errorDestination, messageCreator);
				}
				// TODO consider pausing for several minutes if there are too many failed messages
				return; // return cleanly so that this message is removed from the request queue
			} else {
				// increment retries and send to retry queue
				setRetries(message, retries + 1);
				if (this.retryDestination == null) {
					this.retryJmsOperations.send(messageCreator);
				} else {
					this.retryJmsOperations.send(this.retryDestination, messageCreator);
				}
			}
			/* if anything in this catch block throws an exception, the JMS server should requeue the message automatically, but without incrementing
			 * the retry count
			 */
			// TODO replace with server-enabled retry counting after it's available in OpenMQ (planned for version 4.6)
		}
	}

	private void setRetries(final Message message, final int retries) {
		try {
			final Map<String, Object> propsCopy = new HashMap<String, Object>();
			for (@SuppressWarnings("unchecked")
			final Enumeration<String> propNames = message.getPropertyNames(); propNames.hasMoreElements();) {
				final String curProp = propNames.nextElement();
				if (RETRIES_PROPERTY_NAME.equals(curProp)) {
					continue;
				}
				propsCopy.put(curProp, message.getObjectProperty(curProp));
			}
			message.clearProperties();
			message.setIntProperty(RETRIES_PROPERTY_NAME, retries);
			for (final Entry<String, Object> curProp : propsCopy.entrySet()) {
				message.setObjectProperty(curProp.getKey(), curProp.getValue());
			}
		} catch (final JMSException e) {
			throw new RuntimeException("Could not increment retry count property for name " + RETRIES_PROPERTY_NAME, e);
		}
	}

	private int getRetries(final Message message) {
		try {
			final Object retries = message.getObjectProperty(RETRIES_PROPERTY_NAME);
			if (retries == null) {
				return 0;
			}
			if (retries instanceof Integer) {
				return ((Integer) retries) < 0 ? 0 : (Integer) retries;
			}
			log.warn("Unexpected type for retries property: " + retries.getClass().getName());
			return 0;
		} catch (final JMSException e) {
			throw new RuntimeException("Problem accessing message properties", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		if (this.errorJmsOperations == null) {
			log.warn("No separate error queue set for consistently failing messages. Will use retry queue instead.");
			this.errorJmsOperations = this.retryJmsOperations;
			this.errorDestination = this.retryDestination;
		} else {
			if (this.errorJmsOperations instanceof JmsTemplate) {
				final JmsTemplate errorTemplate = (JmsTemplate) this.errorJmsOperations;
				if (errorTemplate.getDefaultDestination() == null && errorTemplate.getDefaultDestinationName() == null) {
					Assert.notNull(this.errorDestination, "An error destination must be provided if a default isn't set on the template");
				}
			}
		}
		Assert.notNull(this.retryJmsOperations, "A retry template must be provided.");
		if (this.retryJmsOperations instanceof JmsTemplate) {
			final JmsTemplate retryTemplate = (JmsTemplate) this.retryJmsOperations;
			if (retryTemplate.getDefaultDestination() == null && retryTemplate.getDefaultDestinationName() == null) {
				Assert.notNull(this.retryDestination, "A retry destination must be provided if a default isn't set on the template");
			}
		}
	}

	/**
	 * @param errorDestination
	 *            The Destination to use for dead messages, if a default isn't
	 *            set for the error JmsOperations object, or a destination
	 *            different from the default should be used
	 */
	public void setErrorDestination(final Destination errorDestination) {
		this.errorDestination = errorDestination;
	}

	/**
	 * @param retryDestination
	 *            The Destination to use for retrying messages, if a default
	 *            isn't set for the retry JmsOperations object, or a destination
	 *            different from the default should be used
	 */
	public void setRetryDestination(final Destination retryDestination) {
		this.retryDestination = retryDestination;
	}

	public JmsOperations getRetryJmsTemplate() {
		return this.retryJmsOperations;
	}

	public void setRetryJmsTemplate(final JmsOperations retryJmsOperations) {
		this.retryJmsOperations = retryJmsOperations;
	}

	public JmsOperations getErrorJmsTemplate() {
		return this.errorJmsOperations;
	}

	public void setErrorJmsTemplate(final JmsOperations errorJmsOperations) {
		this.errorJmsOperations = errorJmsOperations;
	}

	public int getMaxRetries() {
		return this.maxRetries;
	}

	public void setMaxRetries(final int maxRetries) {
		this.maxRetries = maxRetries;
	}

}
