package com.ontotext.kafka.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

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
	public void ensureValid(String s, Object o) {

		if (o instanceof String) {
			if (!validEnums.contains(((String) o).toLowerCase())) {
				throw new ConfigException(s,
						String.format("'%s' is not a valid value for %s. Valid values are %s.",
								o, enumClass.getSimpleName(),
								validEnums));
			}
		} else {
			throw new ConfigException(s,
					o, "Must be a String");
		}
	}

	@Override
	public String toString() {
		return this.validEnums.stream()
				.sorted()
				.collect(Collectors.joining(", "));
	}
}
