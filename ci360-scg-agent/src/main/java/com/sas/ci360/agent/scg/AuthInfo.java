package com.sas.ci360.agent.scg;

public class AuthInfo {
    static final int DEFAULT_RETRIES = 1;

    private final String key;
    private final String secret;
    private String token;
    private int retries = DEFAULT_RETRIES;
    
    public AuthInfo(final String key,
            final String secret,
            final String token,
            int retries) {
        this.key = key;
        this.secret = secret;
        this.token = token;
        this.retries = retries;
    }
    
    public AuthInfo(final String key,
            final String secret,
            final String token) {
        this(key, secret, token, DEFAULT_RETRIES);
    }

    
	public String getKey() {
		return key;
	}

	public String getSecret() {
		return secret;
	}

	public String getToken() {
		return token;
	}

	public int getRetries() {
		return retries;
	}
    
    
}
