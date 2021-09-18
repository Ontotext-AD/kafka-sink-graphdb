package com.ontotext.kafka.operations;

import org.apache.kafka.connect.runtime.errors.Operation;

public interface OperationHandler {
	<E> E execAndRetry(Operation<E> operation);
}
