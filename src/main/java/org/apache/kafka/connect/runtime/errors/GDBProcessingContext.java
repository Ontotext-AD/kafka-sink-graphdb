package org.apache.kafka.connect.runtime.errors;

public class GDBProcessingContext<T> extends ProcessingContext<T> {

	/**
	 * Construct a context associated with the processing of a particular record
	 *
	 * @param original The original record before processing, as received from either Kafka or a Source Task
	 */
	public GDBProcessingContext(T original) {
		super(original);
	}
}
