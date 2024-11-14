/* Copyright © 2021, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.sas.ci360.agent.exceptions;

public class ConfigurationException extends Exception {
	private static final long serialVersionUID = 1L;	

	public ConfigurationException(String errorMessage) {
		super(errorMessage);
	}
	
	public ConfigurationException(String errorMessage, Throwable ex) {
		super(errorMessage, ex);
	}
	
}
