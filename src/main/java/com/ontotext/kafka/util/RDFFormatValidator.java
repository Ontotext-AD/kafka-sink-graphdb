package com.ontotext.kafka.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class RDFFormatValidator implements ConfigDef.Validator {
	@Override
	public void ensureValid(final String key, final Object value) {

		if (value instanceof String) {
			try {
				ValueUtil.getRDFFormat((String) value);
			} catch (IllegalArgumentException e) {
				throw new ConfigException(key, value, e.getMessage());
			}
		} else {
			throw new ConfigException(key, value, "Must be a String");
		}
	}

	@Override
	public String toString() {
		return "Valid RDF Format";
	}
}

