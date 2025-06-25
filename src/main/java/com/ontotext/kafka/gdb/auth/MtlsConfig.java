package com.ontotext.kafka.gdb.auth;

public class MtlsConfig {
	private final String certificate;
	private final String key;
	private final String keyPassword;

	public MtlsConfig(String certificate, String key, String keyPassword) {
		this.certificate = certificate;
		this.key = key;
		this.keyPassword = keyPassword;
	}

	public String getCertificate() {
		return certificate;
	}

	public String getKey() {
		return key;
	}

	public String getKeyPassword() {
		return keyPassword;
	}
}
