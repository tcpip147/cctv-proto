package com.tcpip147.signal.security;

import java.util.Collection;

import org.jspecify.annotations.Nullable;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;

public class SecurityAuthentication extends AbstractAuthenticationToken {

	private static final long serialVersionUID = 1L;

	private final Object principal;
	private final Object credentials;

	protected SecurityAuthentication(Object principal, Object credentials) {
		super(AuthorityUtils.NO_AUTHORITIES);
		this.principal = principal;
		this.credentials = credentials;
		setAuthenticated(false);
	}

	protected SecurityAuthentication(Object principal, Object credentials,
			Collection<? extends GrantedAuthority> authorities) {
		super(authorities);
		this.principal = principal;
		this.credentials = credentials;
		setAuthenticated(true);
	}

	@Override
	public @Nullable Object getCredentials() {
		return credentials;
	}

	@Override
	public @Nullable Object getPrincipal() {
		return principal;
	}
}
