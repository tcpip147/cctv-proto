package com.tcpip147.signal;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class SignalEnvironment {

	private final Environment environment;

	public SignalEnvironment(Environment environment) {
		this.environment = environment;
	}

	public String getProperty(String key) {
		return environment.getProperty(key);
	}

}
