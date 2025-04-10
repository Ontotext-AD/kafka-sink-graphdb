package com.ontotext.kafka.gdb;

import org.apache.kafka.common.config.ConfigException;

public class GdbConnectionConfigException extends ConfigException {


	private final String valueName;

	public GdbConnectionConfigException(String name, Object value, String message) {
		super(name, value, message);
		this.valueName = name;
	}

	public String getValueName() {
		return valueName;
	}
}
