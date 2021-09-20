package com.ontotext.kafka.operation;

import org.apache.kafka.connect.runtime.errors.Operation;

public interface OperationHandler {
	<E> E execAndRetry(Operation<E> operation);
}
