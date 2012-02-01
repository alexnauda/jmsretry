package com.alexnauda.jmsretry;

import java.io.Serializable;

public interface RetryCount extends Serializable {

	public int getRetryCount();

	public void setRetryCount(int retryCount);

}
