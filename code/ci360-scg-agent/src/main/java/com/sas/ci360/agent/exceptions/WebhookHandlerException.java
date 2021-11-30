/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.sas.ci360.agent.exceptions;

public class WebhookHandlerException extends Exception {
	public static final boolean IS_RETRYABLE = true;
	public static final boolean NOT_RETRYABLE = false;
	
	private static final long serialVersionUID = 1L;	
	private boolean retryable = true;

	public WebhookHandlerException(String errorMessage) {
		super(errorMessage);
	}
	
	public WebhookHandlerException(String errorMessage, boolean retryable) {
		super(errorMessage);
		this.retryable = retryable;
	}
	
	public WebhookHandlerException(String errorMessage, Throwable ex) {
		super(errorMessage, ex);
	}
	
	public WebhookHandlerException(String errorMessage, boolean retryable, Throwable ex) {
		super(errorMessage, ex);
		this.retryable = retryable;
	}

	public boolean isRetryable() {
		return retryable;
	}
	
}
