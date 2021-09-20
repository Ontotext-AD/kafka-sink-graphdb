package com.ontotext.kafka.error;

public class UnToleratedException extends RuntimeException {

	public UnToleratedException(String message, Throwable cause) {
		super(message, cause);
	}
}
