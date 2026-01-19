/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ontotext.kafka.logging;

import org.slf4j.MDC;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A utility for defining Mapped Diagnostic Context (MDC) for SLF4J logs.
 *
 * <p>{@link LoggingContext} instances should be created in a try-with-resources block to ensure
 * that the logging context is properly closed. The only exception is the logging context created
 * upon thread creation that is to be used for the entire lifetime of the thread.
 *
 * <p>Any logger created on the thread will inherit the MDC context, so this mechanism is ideal for
 * providing additional information in the log messages without requiring connector
 * implementations to use a specific Connect API or SLF4J API. {@link LoggingContext#close()}
 * will also properly restore the Connect MDC parameters to their state just prior to when the
 * LoggingContext was created. Use {@link #clear()} to remove all MDC parameters from the
 * current thread context.
 *
 * <p>Compare this approach to {@link org.apache.kafka.common.utils.LogContext}, which must be
 * used to create a new {@link org.slf4j.Logger} instance pre-configured with the desired prefix.
 * Currently, LogContext does not allow the prefix to be changed, and it requires that all
 * components use the LogContext to create their Logger instance.
 */
public final class LoggingContext implements AutoCloseable {

	/**
	 * The name of the Mapped Diagnostic Context (MDC) key that defines the context for a connector.
	 */
	public static final String CONNECTOR_CONTEXT = "connector.context";

	public static final Collection<String> ALL_CONTEXTS = Collections.singleton(CONNECTOR_CONTEXT);


	/**
	 * Clear all MDC parameters.
	 */
	public static void clear() {
		MDC.clear();
	}

	/**
	 * Modify the current {@link MDC} logging context to set the {@link #CONNECTOR_CONTEXT connector context} to include the
	 * supplied name and additional context items.
	 *
	 * @param ctxItems The context items, can be either single item string, or key=value
	 */
	public static LoggingContext withContext(String... ctxItems) {
		Objects.requireNonNull(ctxItems);
		LoggingContext context = new LoggingContext();
		MDC.put(CONNECTOR_CONTEXT, prefixFor(ctxItems));
		return context;
	}


	/**
	 * Return the prefix that uses the specified connector name and additional (optional) items. The
	 * format is as follows:
	 *
	 * <pre>
	 *     [&lt;connectorName>|&lt;[items],>]&lt;sp>
	 * </pre>
	 * <p>
	 * where "<code>&lt;connectorName></code>" is the name of the connector,
	 * "<code>&lt;sp></code>" indicates a trailing space, and
	 * "<code>&lt;items></code>" are additional context items
	 *
	 * @param ctxItems The context items
	 * @return the prefix; never null
	 */
	static String prefixFor(String... ctxItems) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		if (ctxItems != null) {
			sb.append("|");
			for (int i = 0; i < ctxItems.length; ++i) {
				String m = ctxItems[i];
				sb.append(m);
				if (i != ctxItems.length - 1) {
					sb.append(",");
				}
			}

		}
		sb.append("] ");
		return sb.toString();
	}

	private final Map<String, String> previous;

	private LoggingContext() {
		previous = MDC.getCopyOfContextMap(); // may be null!
	}

	/**
	 * Close this logging context, restoring the Connect {@link MDC} parameters back to the state
	 * just before this context was created. This does not affect other MDC parameters set by
	 * connectors or tasks.
	 */
	@Override
	public void close() {
		for (String param : ALL_CONTEXTS) {
			if (previous != null && previous.containsKey(param)) {
				MDC.put(param, previous.get(param));
			} else {
				MDC.remove(param);
			}
		}
	}
}
