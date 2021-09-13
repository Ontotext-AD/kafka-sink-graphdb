package com.ontotext.kafka.error;

import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Error Handling API for records that were not flushed properly to a GraphDB {@link org.eclipse.rdf4j.repository.http.HTTPRepository}
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public interface ErrorHandler {

	void handleFailingRecord(SinkRecord record, Throwable ex);

	<E> E handleRetry(Operation<E> operation);
}
