package com.tcpip147.signal.security;

import java.io.IOException;

import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class SecurityFilter extends OncePerRequestFilter {

	private final AuthenticationManager manager;

	public SecurityFilter(AuthenticationManager manager) {
		this.manager = manager;
	}

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {
		Authentication authRequest = new SecurityAuthentication("", "");
		Authentication authResult = manager.authenticate(authRequest);
		SecurityContextHolder.getContext().setAuthentication(authResult);
		filterChain.doFilter(request, response);
	}
}
