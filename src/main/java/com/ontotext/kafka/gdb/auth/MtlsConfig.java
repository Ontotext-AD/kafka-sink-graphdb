package com.ontotext.kafka.gdb.auth;

import org.apache.kafka.common.config.types.Password;

public class MtlsConfig {
	private final String certificate;
	private final String key;
	private final Password keyPassword;

	public MtlsConfig(String certificate, String key, Password keyPassword) {
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

	public Password getKeyPassword() {
		return keyPassword;
	}
}
