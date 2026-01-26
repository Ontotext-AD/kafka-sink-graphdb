package com.ontotext.kafka.logging;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/**
 * Constructs named loggers based on where the sink connector is deployed.
 * By default, a standard {@link Logger} instance is created for a given class name. When the connector is deployed in AWS MSK, however,
 * a custom `{@link MskLogger} is created to handle the suppression of DEBUG and TRACE messages by AWS MSK cluster environment.
 * <p>
 * To use {@link MskLogger} the connector must be configured with <b>logger.type</b> property set to <b>msk</b>
 *
 * @see com.ontotext.kafka.GraphDBSinkConfig
 *
 */
public class LoggerFactory {

	public static String LOGGER_TYPE_PROPERTY = "loggerType";
	public static String LOG_LEVEL_PROPERTY = "logLevel";

	public enum LoggerType {
		MSK,
		DEFAULT;

		public static LoggerType getLoggerType(String val) {
			if (StringUtils.equalsIgnoreCase("MSK", val)) {
				return MSK;
			}
			return DEFAULT;

		}
	}

	public static Logger getLogger(Class<?> clazz) {
		switch (LoggerType.getLoggerType(System.getProperty(LOGGER_TYPE_PROPERTY))) {
			case MSK:
				return new MskLogger(clazz);
			case DEFAULT:
			default:
				return org.slf4j.LoggerFactory.getLogger(clazz);
		}

	}


}
