package com.tcpip147.signal.signaling;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SignalRequestMessage {

	private String requestId;
	private String type;
	private Map<String, Object> payload;

	public SignalRequestMessage(String type) {
		this.requestId = UUID.randomUUID().toString();
		this.type = type;
		this.payload = new HashMap<>();
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
	}

	public void put(String key, Object value) {
		payload.put(key, value);
	}

	public Object get(String key) {
		return payload.get(key);
	}
}
