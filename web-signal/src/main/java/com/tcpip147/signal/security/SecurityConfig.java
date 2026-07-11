package com.tcpip147.signal.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

	@Bean
	public AuthenticationManager authenticationManager(AuthenticationConfiguration config) {
		return config.getAuthenticationManager();
	}

	@Bean
	public SecurityFilter securityFilter(AuthenticationManager manager) {
		return new SecurityFilter(manager);
	}

	@Bean
	public SecurityFilterChain securityFilterChain(HttpSecurity http, SecurityFilter filter) throws Exception {
		http.csrf(csrf -> csrf.disable()).addFilterBefore(filter, UsernamePasswordAuthenticationFilter.class)
				.authorizeHttpRequests(
						auth -> auth.requestMatchers("/signal/**").permitAll().anyRequest().authenticated());
		return http.build();
	}
}
