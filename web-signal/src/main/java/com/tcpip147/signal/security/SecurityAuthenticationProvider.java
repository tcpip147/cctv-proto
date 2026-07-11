package com.tcpip147.signal.security;

import org.jspecify.annotations.Nullable;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

@Component
public class SecurityAuthenticationProvider implements AuthenticationProvider {

	@Override
	public @Nullable Authentication authenticate(Authentication authentication) throws AuthenticationException {
		String principal = (String) authentication.getPrincipal();
		String credentials = (String) authentication.getCredentials();
		return new SecurityAuthentication(principal, credentials, null);
	}

	@Override
	public boolean supports(Class<?> authentication) {
		return SecurityAuthentication.class.isAssignableFrom(authentication);
	}

}
