package com.ontotext.kafka.validation;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.LinkedHashSet;
import java.util.Set;

public class ValidEnum implements ConfigDef.Validator {
	final Set<String> validEnums;
	final Class<?> enumClass;

	public static ValidEnum of(Class<?> enumClass) {
		return new ValidEnum(enumClass);
	}

	private ValidEnum(Class<?> enumClass) {
		Set<String> validEnums = new LinkedHashSet<>();
		for (Object o : enumClass.getEnumConstants()) {
			String key = o.toString().toLowerCase();
			validEnums.add(key);
		}
		this.validEnums = validEnums;
		this.enumClass = enumClass;
	}

	@Override
	public void ensureValid(final String key, final Object o) {

		if (o instanceof String) {
			if (!validEnums.contains(((String) o).toLowerCase())) {
				throw new ConfigException(key, o, String.format("'%s' is not a valid value for %s. Should be one of: %s",
								o, enumClass.getSimpleName(),
								validEnums));
			}
		} else {
			throw new ConfigException(key, o, "Must be a String");
		}
	}

	@Override
	public String toString() {
		return this.validEnums.toString();
	}
}
