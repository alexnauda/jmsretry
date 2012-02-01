package com.alexnauda.jmsretry;

public class RetryCountMessage implements RetryCount {

	/**
	 * serial version 1: initial version
	 */
	private static final long serialVersionUID = 1L;
	protected int retryCount = 0;

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(final int _retryCount) {
		retryCount = _retryCount;
	}

}
