package com.ontotext.kafka.logging;

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.simple.SimpleLogger;

import static com.ontotext.kafka.logging.LoggerFactory.LOG_LEVEL_PROPERTY;

/**
 * An enriched logger that transforms TRACE messages to INFO and DEBUG to INFO. These messages are logged only if their original level is enabled
 */
public class MskLogger extends SimpleLogger {

	private final Logger log;


	public MskLogger(Class<?> clazz) {
		super(clazz.getSimpleName());
		this.log = org.slf4j.LoggerFactory.getLogger(clazz);
		this.currentLogLevel = stringToLevel(System.getProperty(LOG_LEVEL_PROPERTY));
	}

	@Override
	public void info(String msg) {
		log.info(msg);
	}

	@Override
	public void info(String format, Object arg) {
		log.info(format, arg);
	}

	@Override
	public void info(String format, Object arg1, Object arg2) {
		log.info(format, arg1, arg2);
	}

	@Override
	public void info(String format, Object... arguments) {
		log.info(format, arguments);
	}

	@Override
	public void info(String msg, Throwable t) {
		log.info(msg, t);
	}

	@Override
	public void info(Marker marker, String msg) {
		log.info(marker, msg);
	}

	@Override
	public void info(Marker marker, String format, Object arg) {
		log.info(marker, format, arg);
	}

	@Override
	public void info(Marker marker, String format, Object arg1, Object arg2) {
		log.info(marker, format, arg1, arg2);
	}

	@Override
	public void info(Marker marker, String format, Object... arguments) {
		log.info(marker, format, arguments);
	}

	@Override
	public void info(Marker marker, String msg, Throwable t) {
		log.info(marker, msg, t);
	}

	@Override
	public void warn(String msg) {
		log.warn(msg);
	}

	@Override
	public void warn(String format, Object arg) {
		log.warn(format, arg);
	}

	@Override
	public void warn(String format, Object arg1, Object arg2) {
		log.warn(format, arg1, arg2);
	}

	@Override
	public void warn(String format, Object... arguments) {
		log.warn(format, arguments);
	}

	@Override
	public void warn(String msg, Throwable t) {
		log.warn(msg, t);
	}

	@Override
	public void warn(Marker marker, String msg) {
		log.warn(marker, msg);
	}

	@Override
	public void warn(Marker marker, String format, Object arg) {
		log.warn(marker, format, arg);
	}

	@Override
	public void warn(Marker marker, String format, Object arg1, Object arg2) {
		log.warn(marker, format, arg1, arg2);
	}

	@Override
	public void warn(Marker marker, String format, Object... arguments) {
		log.warn(marker, format, arguments);
	}

	@Override
	public void warn(Marker marker, String msg, Throwable t) {
		log.warn(marker, msg, t);
	}

	@Override
	public void error(String msg) {
		log.error(msg);
	}

	@Override
	public void error(String format, Object arg) {
		log.error(format, arg);
	}

	@Override
	public void error(String format, Object arg1, Object arg2) {
		log.error(format, arg1, arg2);
	}

	@Override
	public void error(String format, Object... arguments) {
		log.error(format, arguments);
	}

	@Override
	public void error(String msg, Throwable t) {
		log.error(msg, t);
	}

	@Override
	public void error(Marker marker, String msg) {
		log.error(marker, msg);
	}

	@Override
	public void error(Marker marker, String format, Object arg) {
		log.error(marker, format, arg);
	}

	@Override
	public void error(Marker marker, String format, Object arg1, Object arg2) {
		log.error(marker, format, arg1, arg2);
	}

	@Override
	public void error(Marker marker, String format, Object... arguments) {
		log.error(marker, format, arguments);
	}

	@Override
	public void error(Marker marker, String msg, Throwable t) {
		log.error(marker, msg, t);
	}

	@Override
	public void trace(String msg) {
		if (isTraceEnabled()) {
			log.info(msg);
		}
	}

	@Override
	public void trace(String format, Object arg) {
		if (isTraceEnabled()) {
			log.info(format, arg);
		}
	}

	@Override
	public void trace(String format, Object arg1, Object arg2) {
		if (isTraceEnabled()) {
			log.info(format, arg1, arg2);
		}
	}

	@Override
	public void trace(String format, Object... arguments) {
		if (isTraceEnabled()) {
			log.info(format, arguments);
		}
	}

	@Override
	public void trace(String msg, Throwable t) {
		if (isTraceEnabled()) {
			log.info(msg, t);
		}
	}

	@Override
	public void trace(Marker marker, String msg) {
		if (isTraceEnabled(marker)) {
			log.info(marker, msg);
		}
	}

	@Override
	public void trace(Marker marker, String format, Object arg) {
		if (isTraceEnabled(marker)) {
			log.info(marker, format, arg);
		}
	}

	@Override
	public void trace(Marker marker, String format, Object arg1, Object arg2) {
		if (isTraceEnabled(marker)) {
			log.info(marker, format, arg1, arg2);
		}
	}

	@Override
	public void trace(Marker marker, String format, Object... argArray) {
		if (isTraceEnabled(marker)) {
			log.info(marker, format, argArray);
		}
	}

	@Override
	public void trace(Marker marker, String msg, Throwable t) {
		if (isTraceEnabled(marker)) {
			log.info(marker, msg, t);
		}
	}

	@Override
	public void debug(String msg) {
		if (isDebugEnabled()) {
			log.info(msg);
		}
	}

	@Override
	public void debug(String format, Object arg) {
		if (isDebugEnabled()) {
			log.info(format, arg);
		}
	}

	@Override
	public void debug(String format, Object arg1, Object arg2) {
		if (isDebugEnabled()) {
			log.info(format, arg1, arg2);
		}
	}

	@Override
	public void debug(String format, Object... arguments) {
		if (isDebugEnabled()) {
			log.info(format, arguments);
		}
	}

	@Override
	public void debug(String msg, Throwable t) {
		if (isDebugEnabled()) {
			log.info(msg, t);
		}
	}

	@Override
	public void debug(Marker marker, String msg) {
		if (isDebugEnabled(marker)) {
			log.info(marker, msg);
		}
	}

	@Override
	public void debug(Marker marker, String format, Object arg) {
		if (isDebugEnabled(marker)) {
			log.info(marker, format, arg);
		}
	}

	@Override
	public void debug(Marker marker, String format, Object arg1, Object arg2) {
		if (isDebugEnabled(marker)) {
			log.info(marker, format, arg1, arg2);
		}
	}

	@Override
	public void debug(Marker marker, String format, Object... arguments) {
		if (isDebugEnabled(marker)) {
			log.info(marker, format, arguments);
		}
	}

	@Override
	public void debug(Marker marker, String msg, Throwable t) {
		if (isDebugEnabled(marker)) {
			log.info(marker, msg, t);
		}
	}

	private static int stringToLevel(String levelStr) {
		if ("trace".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_TRACE;
		} else if ("debug".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_DEBUG;
		} else if ("info".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_INFO;
		} else if ("warn".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_WARN;
		} else if ("error".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_ERROR;
		} else if ("off".equalsIgnoreCase(levelStr)) {
			return SimpleLogger.LOG_LEVEL_OFF;
		}
		// assume INFO by default
		return SimpleLogger.LOG_LEVEL_INFO;
	}

}

