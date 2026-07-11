package com.tcpip147.signal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SignalApplication {

	public static void main(String[] args) {
		SpringApplication.run(SignalApplication.class, args);
	}

}
