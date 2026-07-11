package com.tcpip147.signal.signaling;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class SignalWebSocketConfig implements WebSocketConfigurer {

	private final SignalWebSocketHandler handler;

	public SignalWebSocketConfig(SignalWebSocketHandler handler) {
		this.handler = handler;
	}

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(handler, "/signal").setAllowedOrigins("*")
				.addInterceptors(new SignalHandshakeInterceptor());
	}

}
