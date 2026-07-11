package com.tcpip147.signal.signaling;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class SignalWebSocketHandler extends TextWebSocketHandler {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final SignalWebSocketClient client;
	private final List<String> bypass = List.of("getLeastLoadedConsumerHub", "getRouterRtpCapabilities",
			"createWebRtcTransport", "createConsumer", "connectWebRtcTransport", "resumeConsumer");

	public SignalWebSocketHandler(SignalWebSocketClient client) {
		this.client = client;
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		SignalRequestMessage request = objectMapper.readValue(message.getPayload(), SignalRequestMessage.class);

		if (bypass.contains(request.getType())) {
			client.send(request).whenComplete((response, _) -> {
				try {
					session.sendMessage(new TextMessage(objectMapper.writeValueAsBytes(response)));
				} catch (JacksonException | IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
	}

}
