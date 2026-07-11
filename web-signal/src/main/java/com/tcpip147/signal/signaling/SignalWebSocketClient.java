package com.tcpip147.signal.signaling;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.tcpip147.signal.SignalEnvironment;

import tools.jackson.databind.ObjectMapper;

@Service
public class SignalWebSocketClient {

	private final SignalEnvironment environment;
	private final Object lock = new Object();
	private final StandardWebSocketClient client = new StandardWebSocketClient();
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ConcurrentMap<String, CompletableFuture<SignalResponseMessage>> pendingRequests = new ConcurrentHashMap<>();
	private volatile WebSocketSession socketSession;
	private boolean connecting;
	private final WebSocketHandler handler = new TextWebSocketHandler() {
		@Override
		public void afterConnectionEstablished(WebSocketSession session) throws Exception {
			synchronized (lock) {
				socketSession = new ConcurrentWebSocketSessionDecorator(session, 5000, 1024 * 1024);
				connecting = false;
			}
		}

		@Override
		protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
			SignalResponseMessage response = objectMapper.readValue(message.getPayload(), SignalResponseMessage.class);
			CompletableFuture<SignalResponseMessage> waiting = pendingRequests.remove(response.getRequestId());
			if (waiting != null) {
				waiting.complete(response);
			}
		}

		@Override
		public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
			dropResources(session);
		}

		@Override
		public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
			dropResources(session);
		}
	};

	public SignalWebSocketClient(SignalEnvironment environment) {
		this.environment = environment;
	}

	private void dropResources(WebSocketSession session) {
		synchronized (lock) {
			if (socketSession != null && socketSession.getId().equals(session.getId())) {
				socketSession = null;
				pendingRequests.forEach((_, future) -> future
						.completeExceptionally(new IllegalStateException("WebSocket connection closed")));
				pendingRequests.clear();
			}
			connecting = false;
		}
	}

	@Scheduled(fixedDelay = 3000)
	private void connect() {
		synchronized (lock) {
			if (socketSession != null || connecting) {
				return;
			}
			connecting = true;
		}
		try {
			CompletableFuture<WebSocketSession> future = client.execute(handler, environment.getProperty("sfu.url"));
			future.whenComplete((_, ex) -> {
				if (ex != null) {
					synchronized (lock) {
						connecting = false;
					}
				}
			});
		} catch (RuntimeException e) {
			synchronized (lock) {
				connecting = false;
			}
		}
	}

	public CompletableFuture<SignalResponseMessage> send(SignalRequestMessage request) {
		WebSocketSession session;
		String requestId = request.getRequestId();
		CompletableFuture<SignalResponseMessage> future = new CompletableFuture<>();

		synchronized (lock) {
			session = socketSession;
			if (session == null || !session.isOpen()) {
				return CompletableFuture.failedFuture(new IllegalStateException("SFU WebSocket is not connected"));
			}

			CompletableFuture<SignalResponseMessage> prev = pendingRequests.putIfAbsent(requestId, future);
			if (prev != null) {
				return CompletableFuture.failedFuture(new IllegalStateException("Duplicate requestId: " + requestId));
			}
		}

		try {
			String message = objectMapper.writeValueAsString(request);
			session.sendMessage(new TextMessage(message));
		} catch (IOException e) {
			if (pendingRequests.remove(requestId, future)) {
				future.completeExceptionally(e);
			}
		}

		return future.orTimeout(5, TimeUnit.SECONDS).whenComplete((_, _) -> pendingRequests.remove(requestId, future));
	}
}
