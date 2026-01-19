package com.ontotext.kafka.logging;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

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
