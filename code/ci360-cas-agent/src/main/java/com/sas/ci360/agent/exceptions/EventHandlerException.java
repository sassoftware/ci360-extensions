package com.sas.ci360.agent.exceptions;

public class EventHandlerException extends Exception {
	public static final boolean IS_RETRYABLE = true;
	public static final boolean NOT_RETRYABLE = false;
	
	private static final long serialVersionUID = 1L;	
	private boolean retryable = true;

	public EventHandlerException(String errorMessage) {
		super(errorMessage);
	}
	
	public EventHandlerException(String errorMessage, boolean retryable) {
		super(errorMessage);
		this.retryable = retryable;
	}
	
	public EventHandlerException(String errorMessage, Throwable ex) {
		super(errorMessage, ex);
	}
	
	public EventHandlerException(String errorMessage, boolean retryable, Throwable ex) {
		super(errorMessage, ex);
		this.retryable = retryable;
	}

	public boolean isRetryable() {
		return retryable;
	}
	
}
