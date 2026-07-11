package com.tcpip147.signal.signaling;

import java.util.Map;

public class SignalResponseMessage {

	private String requestId;
	private Map<String, Object> payload;
	private Map<String, Object> error;

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
	}

	public Map<String, Object> getError() {
		return error;
	}

	public void setError(Map<String, Object> error) {
		this.error = error;
	}
}
