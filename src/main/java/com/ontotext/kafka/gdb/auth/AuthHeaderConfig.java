package com.ontotext.kafka.gdb.auth;

public class AuthHeaderConfig {
	private final String certificate;
	private final String headerName;

	public AuthHeaderConfig(String certificate, String headerName) {
		this.certificate = certificate;
		this.headerName = headerName;
	}

	public String getCertificate() {
		return certificate;
	}

	public String getHeaderName() {
		return headerName;
	}
}
